# LAKEHOUSE CLIENTS BANKNG TRANSACTIONS RESUME 


## French version ------------------------------------------------------------------------------------------------
### Introduction 

Ce projet met en evidence un use case très frequent dans les fintechs.
Les fintech sont des entreprise qui offre des solutions de financiere a destination de particuliers de differents types suivant leurs business modèle (B2B, B2C).
Pour les fintech offrant des systemes de collecte de fonds,  elles vont forcément inclure un suivit en temps réelle ou pseudo-temps réelle du solde du clients, pour renforcer leurs image de marques d'entreprise financière serieuse. Mais il faut dire que pour des solutions de fintech basée essentiellement sur du CLOUD COMPUTING. Les solutions de LAKEHOUSE sont veritablement une obaine pour limiter les couts de d'Input / Ouput en base de données pour un suivi des points d'encaissement.

Dans ce projet nous montrons un POC (Proof Of Concept) de mise en place d'un lakehouse from scratch et free.
La stack complète se compose d'outils moderne en autre  :  
- MINIO : Stockage objet
- ICEBERG :  Format de table
- CLUSTER SPARK : Transformation & Ingestion des données 
- AIRFLOW : Orchestration du Workflow
- PROMETHEUS  : Scraping des KPI en provenance des agents de collecte
- STATSD OU TELEGRAF :  Agent de collecte
- GRAFANA :  Monitoring infra
- METABASE : Data Vizualisation 

#### ETAPE  1 : 
L'etape un sera une etape d'analyse de données de mise en place de notre dictionnaire de données.
Les données qui font l'objet de cet experience suive le schema avec les colonnes que voici : 
<pre>
- id_clt
</pre>









