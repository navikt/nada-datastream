package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/navikt/nada-datastream/cmd"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

type Client struct {
	clientSet     *kubernetes.Clientset
	dynamicClient *dynamic.DynamicClient
	namespace     string
}

func New(context, namespace string) (*Client, error) {
	kubeConfig, err := getKubeConfig(context, namespace)
	if err != nil {
		return nil, err
	}

	ns, _, err := kubeConfig.Namespace()
	if err != nil {
		return nil, err
	}

	config, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, err
	}

	config.AuthProvider = nil
	config.ExecProvider = &api.ExecConfig{
		Command:            "gke-gcloud-auth-plugin",
		APIVersion:         "client.authentication.k8s.io/v1beta1",
		InstallHint:        "Requires gke-gcloud-auth-plugin",
		ProvideClusterInfo: true,
		InteractiveMode:    api.IfAvailableExecInteractiveMode,
	}

	return &Client{
		clientSet:     kubernetes.NewForConfigOrDie(config),
		dynamicClient: dynamic.NewForConfigOrDie(config),
		namespace:     ns,
	}, nil
}

func (c *Client) DBConfig(ctx context.Context, appName, dbUser string) (cmd.DBConfig, error) {
	dbConf := cmd.DBConfig{
		Port: "5432",
	}

	err := c.setDBInstanceInfo(ctx, appName, &dbConf)
	if err != nil {
		return cmd.DBConfig{}, err
	}

	dbSecret, err := c.getDBSecret(ctx, appName, dbUser)
	if err != nil {
		return cmd.DBConfig{}, err
	}

	for k, v := range dbSecret.Data {
		switch {
		case strings.HasSuffix(k, "USERNAME"):
			dbConf.User = string(v)
		case strings.HasSuffix(k, "PASSWORD"):
			dbConf.Password = string(v)
		case strings.HasSuffix(k, "DATABASE"):
			dbConf.DB = string(v)
		}
	}

	return dbConf, nil
}

func (c *Client) setDBInstanceInfo(ctx context.Context, appName string, dbConf *cmd.DBConfig) error {
	sqlInstances, err := c.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "sql.cnrm.cloud.google.com",
		Version:  "v1beta1",
		Resource: "sqlinstances",
	}).Namespace(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + appName,
	})
	if err != nil {
		return err
	}

	if len(sqlInstances.Items) == 0 {
		return fmt.Errorf("findDBInstance: no sqlinstance found for app %q in %q", appName, c.namespace)
	} else if len(sqlInstances.Items) > 1 {
		return fmt.Errorf("findDBInstance: multiple sqlinstances found for app %q in %q", appName, c.namespace)
	}

	sqlInstance := sqlInstances.Items[0]

	connectionName, ok := sqlInstance.Object["status"].(map[string]interface{})["connectionName"]
	if !ok {
		return fmt.Errorf("missing 'connectionName' status field; run 'kubectl describe sqlinstance %s' and check for status failures", sqlInstance.GetName())
	}

	parts := strings.Split(connectionName.(string), ":")
	if len(parts) != 3 {
		return fmt.Errorf("connection name '%v' has invalid format, should be <project>:<region>:<instance>", connectionName.(string))
	}

	dbConf.Project = parts[0]
	dbConf.Region = parts[1]
	dbConf.Instance = parts[2]

	return nil
}

func (c *Client) getDBSecret(ctx context.Context, appName, dbUser string) (*v1.Secret, error) {
	sqlUsers, err := c.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "sql.cnrm.cloud.google.com",
		Version:  "v1beta1",
		Resource: "sqlusers",
	}).Namespace(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + appName,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to find secrets for app %v", appName)
	}

	type object struct {
		Spec struct {
			Password struct {
				ValueFrom struct {
					SecretKeyRef struct {
						Key  string `json:"key"`
						Name string `json:"name"`
					} `json:"secretKeyRef"`
				} `json:"valueFrom"`
			} `json:"password"`
		} `json:"spec"`
	}

	for _, u := range sqlUsers.Items {
		obj := object{}
		dataBytes, err := u.MarshalJSON()
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(dataBytes, &obj); err != nil {
			return nil, err
		}

		if strings.Contains(obj.Spec.Password.ValueFrom.SecretKeyRef.Key, strings.ToUpper("_"+strings.ReplaceAll(dbUser, "-", "_"))+"_") {
			return c.clientSet.CoreV1().Secrets(c.namespace).Get(ctx, obj.Spec.Password.ValueFrom.SecretKeyRef.Name, metav1.GetOptions{})
		}
	}

	return nil, fmt.Errorf("unable to find db secret for user %v", dbUser)
}

func getKubeConfig(context, namespace string) (clientcmd.ClientConfig, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, nil)

	if context != "" || namespace != "" {
		return replaceConfigDefaults(context, namespace, kubeConfig)
	}

	return kubeConfig, nil
}

func replaceConfigDefaults(context, namespace string, kubeConfig clientcmd.ClientConfig) (clientcmd.ClientConfig, error) {
	apiConfig, err := kubeConfig.RawConfig()
	if err != nil {
		return kubeConfig, err
	}

	if context == "" {
		context = apiConfig.CurrentContext
	}

	if namespace == "" {
		namespace = apiConfig.Contexts[apiConfig.CurrentContext].Namespace
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{
		CurrentContext: context,
		Context: api.Context{
			Namespace: namespace,
		},
	}), nil
}
