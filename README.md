# Nada Datastream
Oppsett av datastream fra en cloudsql postgres database til bigquery.

## Binær utgivelse
[Nedlast binærfil](https://github.com/navikt/nada-datastream/releases)
 Vi tilbyr binær for mac og linux, mens for Windows-brukere, vennligst bruk Linux-delsystemet og den Linux-baserte binærfilen.

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
      diskAutoresize: true # Datastream bruker en del lagringsplass
... 
````

Videre må man legge til en databasemigrasjon som sørger for at den nyopprettede brukeren får tilgang til å lese de tabellene den er avhengig av for datastreamen. 
I eksempelet under er det gitt `select` tilgang for alle tabeller i `public` schema, men dette kan man spisse ytterligere dersom det er ønskelig.

Migrasjonen må også gi den nyopprettede brukeren `REPLICATION` rolle i databasen og lage en [publication og replication slot](https://cloud.google.com/datastream/docs/configure-your-source-postgresql-database#create_a_publication_and_a_replication_slot_2).
````sql
ALTER DEFAULT PRIVILEGES IN SCHEMA PUBLIC GRANT SELECT ON TABLES TO "datastream";
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO "datastream";

ALTER USER "appnavn" WITH REPLICATION;
ALTER USER "datastream" WITH REPLICATION;
CREATE PUBLICATION "ds_publication" FOR ALL TABLES;
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('ds_replication', 'pgoutput');
````
Merk: både appens bruker ("appnavn") og den nye brukeren trenger å oppdateres med `REPLICATION` rollen i databasen over

## Sett opp datastream kobling
Anbefaler at brukeren som skal kjøre oppsettet gir seg midlertidig `Project Editor` rolle i prosjektet.
Dette gjøres i IAM under `Grant Access`: `Role` -> `Basic`-> `Editor`.

Oppsettet krever at man:
    - er koblet til naisdevice
    - har tilgang til clusteret og namespacet som appen kjører i
    - har kjørt `gcloud auth login --update-adc`

Det enkleste er at context (cluster og namespace) allerede er satt i terminalen. 
Det er også mulig å spesifisere dette som script-argumenter: `--context` og `--namespace`

For å sette opp datastream kjør så følgende:

````bash
./bin/nada-datastream create appnavn databasebruker
````

Dersom man ikke spesifiserer noe vil alle tabeller i public schema i databasen inkluderes i streamen. For å ekskludere enkelte tabeller bruk flagget `--exclude-tables` som tar en kommaseparert streng med tabellene man ønsker å utelate, f.eks.

````bash
./bin/nada-datastream create appnavn databasebruker --exclude-tables=tabell1,tabell2,tabell3
````
Tilsvarende kan du også *inkludere* tabeller: Bruk da flagget `--include-tables`.

For flagg se
```bash
./bin/nada-datastream create --help
```

NB! krever gcloud versjon høyere enn 412.0.0, oppdater med `gcloud components update`
