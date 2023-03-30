# Nada Datastream
Oppsett av datastream fra en cloudsql postgres database til bigquery.

## Forutsetninger for bruk
Det er noen steg som må utføres på forhånd for å klargjøre postgres databasen for datastream. Først må man:

- Opprette en ny databasebruker som datastream koblingen skal bruke
- Databaseflagget `cloudsql.logical_decoding` må settes på for at man skal kunne bruke replication slots i databasen.

Disse to punktene løses enklest ved å editere nais manifestet til appen som eksemplifisert under:
````yaml
...
spec:
  gcp:
    sqlInstances:
    - name: myinstance
      flags:
      - name: cloudsql.logical_decoding # flagget som må settes
        value: "on" # flagget som må settes
      databases:
      - name: mydatabase
        users:
        - name: datastream # ekstra databasebruker
... 
````

Videre må man legge til en databasemigrasjon som sørger for at den nyopprettede brukeren får tilgang til å lese de tabellene den er avhengig av for datastreamen. I eksempelet under er det gitt `select` tilgang for alle tabeller i `public` schema, men dette kan man spisse ytterligere dersom det er ønskelig.

Migrasjonen må også gi den nyopprettede brukeren `REPLICATION` rolle i databasen og lage en [publication og replication slot](https://cloud.google.com/datastream/docs/configure-your-source-postgresql-database#create_a_publication_and_a_replication_slot_2).
````sql
ALTER DEFAULT PRIVILEGES IN SCHEMA PUBLIC GRANT SELECT ON TABLES TO "datastream";
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO "datastream";

ALTER USER "appnavn" WITH REPLICATION;
ALTER USER "datastream" WITH REPLICATION;
CREATE PUBLICATION "ds_publication" FOR ALL TABLES;
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('ds_replication', 'pgoutput');
````
Merk: både appens bruker og den nye brukeren trenger å oppdateres med `REPLICATION` rollen i databasen over

## Sett opp datastream kobling
Anbefaler at brukeren som skal kjøre oppsettet gir seg midlertidig `Project Editor` rolle i prosjektet.

For å sette opp datastream kjør så følgende:

````bash
./bin/nada-datastream --project=<gcp prosjekt> --instance=<instans navn> --db=<db navn> --user=<bruker> --password=<passord>
````

NB! krever gcloud versjon høyere enn 412.0.0, oppdater med `gcloud components update`
