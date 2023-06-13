package k8s

import (
	"context"
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
	clientSet        *kubernetes.Clientset
	dynamicClient    *dynamic.DynamicClient
	defaultNamespace string
}

func New() (*Client, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, nil)
	config, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("NewDBInfo: unable to get kubeconfig: %w", err)
	}

	config.AuthProvider = nil
	config.ExecProvider = &api.ExecConfig{
		Command:            "gke-gcloud-auth-plugin",
		APIVersion:         "client.authentication.k8s.io/v1beta1",
		InstallHint:        "Requires gke-gcloud-auth-plugin",
		ProvideClusterInfo: true,
		InteractiveMode:    api.IfAvailableExecInteractiveMode,
	}

	namespace, _, err := kubeConfig.Namespace()
	if err != nil {
		return nil, fmt.Errorf("NewDBConfig: unable to get namespace: %w", err)
	}

	return &Client{
		clientSet:        kubernetes.NewForConfigOrDie(config),
		dynamicClient:    dynamic.NewForConfigOrDie(config),
		defaultNamespace: namespace,
	}, nil
}

func (c *Client) DBConfig(ctx context.Context, appName, dbUser string, namespace string) (cmd.DBConfig, error) {
	if namespace == "" {
		namespace = c.defaultNamespace
	}

	dbConf := cmd.DBConfig{
		Port: "5432",
	}

	err := c.setDBInstanceInfo(ctx, appName, &dbConf, namespace)
	if err != nil {
		return cmd.DBConfig{}, err
	}

	secrets, err := c.clientSet.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return cmd.DBConfig{}, err
	}

	dbSecret, err := getDBSecret(appName, dbUser, secrets)
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

func (c *Client) setDBInstanceInfo(ctx context.Context, appName string, dbConf *cmd.DBConfig, namespace string) error {
	if namespace == "" {
		namespace = c.defaultNamespace
	}

	sqlInstances, err := c.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "sql.cnrm.cloud.google.com",
		Version:  "v1beta1",
		Resource: "sqlinstances",
	}).Namespace(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + appName,
	})
	if err != nil {
		return err
	}

	if len(sqlInstances.Items) == 0 {
		return fmt.Errorf("findDBInstance: no sqlinstance found for app %q in %q", appName, namespace)
	} else if len(sqlInstances.Items) > 1 {
		return fmt.Errorf("findDBInstance: multiple sqlinstances found for app %q in %q", appName, namespace)
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

func getDBSecret(appName, dbUser string, secrets *v1.SecretList) (v1.Secret, error) {
	for _, s := range secrets.Items {
		if isDBSecret(s.Name, appName, dbUser) {
			return s, nil
		}
	}

	return v1.Secret{}, fmt.Errorf("unable to find db secret for user %v", dbUser)
}

func isDBSecret(secretName, appName, dbUser string) bool {
	dbUser = strings.ReplaceAll(dbUser, "_", "-")
	return strings.HasPrefix(secretName, "google-sql-"+appName) && strings.Contains(secretName, dbUser)
}
