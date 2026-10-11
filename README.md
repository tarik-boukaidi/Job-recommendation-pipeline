# Job Intelligence Platform

Plateforme de collecte et d'analyse d'offres d'emploi au Maroc et en France. Le projet combine des scrapers, un pipeline orchestre par Apache Airflow, un entrepot PostgreSQL, un data lake MinIO, des API et une interface React de recherche et de recommandation.

![Architecture de la plateforme](architecture.png)

## Sommaire

- [Fonctionnalites](#fonctionnalites)
- [Architecture et flux de donnees](#architecture-et-flux-de-donnees)
- [Structure du depot](#structure-du-depot)
- [Demarrage](#demarrage)
- [Configuration](#configuration)
- [API](#api)
- [Interface web](#interface-web)
- [Contribution](#contribution)

## Fonctionnalites

- Collecte d'offres depuis Indeed, LinkedIn, Rekrute, Emploi.ma et France Travail.
- Orchestration quotidienne du pipeline avec Airflow.
- Stockage analytique dans PostgreSQL et stockage objet compatible S3 avec MinIO.
- API FastAPI pour consulter les offres, les statistiques et l'etat du pipeline.
- API Flask et modele de recommandation de postes.
- Interface React/TypeScript pour rechercher des offres et consulter les correspondances.

## Architecture et flux de donnees

```text
Sources d'emploi
      |
      v
Scrapers -> fichier JSON de travail
      |
      +----> MinIO : bronze -> silver -> gold
      |
      v
PostgreSQL : jobs_dw.raw_jobs -> jobs_dw.dim_jobs -> vues analytiques
      |
      +----> API FastAPI (port 8000)
      +----> API de recommandation Flask (port 5001)
                         |
                         v
                 Interface React (port 5173)
```

Le DAG `jobs_intelligence_pipeline` est programme chaque jour a 05:00 UTC. Il collecte les offres, alimente le data lake et l'entrepot, exporte les donnees curatees, relance l'entrainement du modele et enregistre le resultat du pipeline.

Les donnees PostgreSQL se trouvent dans le schema `jobs_dw`. Les vues utilisees par l'API comprennent `vw_latest_jobs`, `vw_skill_mentions`, `vw_jobs_by_category_city`, `vw_salary_stats` et `vw_pipeline_health`.

## Structure du depot

```text
.
├── api/
│   ├── Dockerfile               # Image du service FastAPI
│   ├── main.py                  # Routes analytiques et connexion a PostgreSQL/MinIO
│   └── requirements.txt         # Dependances Python de l'API FastAPI
├── dags/
│   ├── .airflowignore           # Exclusions de fichiers pour la decouverte Airflow
│   └── jobs_pipeline_dag.py     # Orchestration Airflow de la collecte au retraining
├── data/
│   └── .gitkeep                 # Garde le dossier vide dans Git ; les donnees restent locales
├── frontend/
│   ├── public/
│   │   ├── favicon.svg          # Favicon de l'application web
│   │   └── icons.svg            # Sprite SVG statique, non reference actuellement
│   ├── src/
│   │   ├── assets/
│   │   │   └── hero.png         # Visuel importe, mais non affiche actuellement
│   │   ├── App.css              # Styles de l'interface principale
│   │   ├── App.tsx              # Ecran de recherche et affichage des recommandations
│   │   ├── index.css            # Styles globaux
│   │   └── main.tsx             # Point d'entree React
│   ├── .env.example             # Exemple de configuration frontend
│   ├── .gitignore               # Exclusions Git propres au frontend
│   ├── eslint.config.js         # Regles de lint JavaScript/TypeScript
│   ├── index.html               # Document HTML racine de Vite
│   ├── package.json             # Dependances et commandes npm
│   ├── package-lock.json        # Versions verrouillees des dependances npm
│   ├── tsconfig.app.json        # Configuration TypeScript de l'application
│   ├── tsconfig.json            # Configuration TypeScript racine et references
│   ├── tsconfig.node.json       # Configuration TypeScript des fichiers Node/Vite
│   └── vite.config.ts           # Configuration du serveur et du build Vite
├── scraper/
│   ├── sources/
│   │   ├── __init__.py          # Marque le dossier comme package Python
│   │   ├── common.py            # Normalisation, classification et utilitaires partages
│   │   ├── emploi_ma.py         # Collecte des offres depuis Emploi.ma
│   │   ├── france_travail.py    # Client de l'API France Travail
│   │   ├── indeed.py            # Connecteur Indeed via JobSpy
│   │   ├── jobspy_common.py     # Logique JobSpy partagee (Indeed et LinkedIn)
│   │   ├── linkedin.py          # Connecteur LinkedIn via JobSpy
│   │   └── rekrute.py           # Collecte des offres depuis Rekrute
│   ├── indeed_scraper.py        # Module de compatibilite importe par le DAG
│   └── job_sources.py           # Active les sources, execute et deduplique les collectes
├── sql/
│   └── init_jobs_dw.sql         # Definition du schema, tables et vues analytiques
├── .env.example                 # Variables d'environnement d'exemple pour Compose
├── .gitattributes               # Regles de fins de ligne et fichiers binaires pour Git
├── .gitignore                   # Fichiers locaux, secrets, donnees et artefacts exclus de Git
├── api_server.py                # API Flask : filtres, matching, sante et retraining
├── architecture.png             # Diagramme d'architecture affiche dans ce README
├── docker-compose.jobs.yaml     # Variante Compose pour les services du pipeline et du matching
├── docker-compose.yaml          # Definition principale des services Docker
├── Dockerfile.airflow           # Image Airflow avec dependances du pipeline
├── Dockerfile.matcher           # Image de l'API de matching et de son modele
├── job_matcher_model_v2.py      # Chargement des donnees, entrainement et inference ML
├── requirements-airflow.txt     # Dependances Python des scrapers et du DAG
├── requirements-matcher.txt     # Dependances Python du modele et de l'API Flask
└── test_api.py                  # Exemple manuel de requete vers l'API de matching
```

Les fichiers de configuration et les donnees d'execution locales sont volontairement separes du code : `.env` et les fichiers de donnees ne doivent pas etre publies sur GitHub. `package-lock.json` sert a installer des versions reproductibles des dependances frontend. A noter : `hero.png` est importe dans `App.tsx`, mais la variable importee n'est pas utilisee ; le sprite `icons.svg` n'a pas non plus de reference dans le frontend.

## Demarrage

### Prerequis

- Docker Desktop avec Docker Compose
- Node.js et npm pour lancer l'interface en mode developpement
- Au moins 4 Go de memoire disponible pour les services Airflow
- Des identifiants France Travail si cette source est activee

### 1. Cloner et configurer le depot

```bash
git clone https://github.com/brahim-chahba/Job-recommendation-pipeline.git
cd Job-recommendation-pipeline
```

Creer le fichier local de configuration :

```powershell
Copy-Item .env.example .env
```

Sous macOS ou Linux :

```bash
cp .env.example .env
```

Modifier `.env` selon votre environnement. Ne jamais committer ce fichier ni y ajouter de vrais secrets.

### 2. Lancer les services de donnees et le pipeline

```bash
docker compose up -d postgres redis minio minio-init airflow-init airflow-apiserver airflow-scheduler airflow-worker airflow-dag-processor airflow-triggerer api
```

Pour consulter l'etat des services :

```bash
docker compose ps
docker compose logs -f
```

| Service | Adresse locale |
|---|---|
| Interface Airflow | http://localhost:8081 |
| API analytique FastAPI | http://localhost:8000 |
| Documentation interactive FastAPI | http://localhost:8000/docs |
| API de matching Flask | http://localhost:5001 |
| MinIO | http://localhost:9000 |
| Console MinIO | http://localhost:9001 |
| PostgreSQL | localhost:5432 |

L'initialisation de la base Airflow est geree par le service `airflow-init` de Compose. Dans Airflow, activer puis declencher le DAG `jobs_intelligence_pipeline` pour executer une premiere collecte.

Cette commande omet volontairement `matcher-api` : son image Docker reference des fichiers qui ne sont pas presents dans le depot. L'interface web peut etre demarree, mais ses fonctions de recommandation necessitent de corriger et de lancer ce service, ainsi que de lui fournir un jeu de donnees.

### 3. Lancer l'interface web

Dans un autre terminal :

```bash
cd frontend
npm ci
npm run dev
```

Ouvrir l'adresse indiquee par Vite, generalement http://localhost:5173. L'interface appelle actuellement l'API de matching sur `http://localhost:5001`.

Pour verifier le build et le lint de l'interface :

```bash
cd frontend
npm run build
npm run lint
```

## Configuration

Les principaux reglages d'execution se trouvent dans [.env.example](.env.example) :

| Variable | Role |
|---|---|
| `ENABLE_JOBSPY` | Active ou desactive la collecte via JobSpy |
| `ENABLE_INDEED` | Active ou desactive Indeed |
| `ENABLE_LINKEDIN` | Active ou desactive LinkedIn |
| `ENABLE_REKRUTE` | Active ou desactive Rekrute |
| `ENABLE_EMPLOI_MA` | Active ou desactive Emploi.ma |
| `ENABLE_FRANCE_TRAVAIL` | Active France Travail ; necessite les identifiants OAuth2 |
| `FRANCE_TRAVAIL_CLIENT_ID` | Identifiant client France Travail |
| `FRANCE_TRAVAIL_CLIENT_SECRET` | Secret client France Travail |
| `ENABLE_MINIO_LAKE` | Active ou desactive l'ecriture dans MinIO |
| `MINIO_ENDPOINT` | Adresse du service MinIO |
| `MINIO_BUCKET` | Nom du bucket du data lake |

Les valeurs de `.env.example` servent uniquement au developpement local. Remplacez les identifiants par defaut avant tout deploiement partage ou public.

## API

### API analytique — FastAPI, port 8000

| Methode | Route | Description |
|---|---|---|
| `GET` | `/health` | Verifie la connexion a PostgreSQL |
| `GET` | `/jobs` | Liste les offres ; filtres possibles : `category`, `city`, `country`, `remote`, `source`, `limit` |
| `GET` | `/skills` | Competences les plus frequemment mentionnees |
| `GET` | `/stats/category-city` | Repartition des offres par categorie et ville |
| `GET` | `/stats/salary` | Statistiques salariales |
| `GET` | `/pipeline-health` | Historique de sante du pipeline |
| `GET` | `/lake-health` | Etat du data lake MinIO |

Exemple :

```text
http://localhost:8000/jobs?city=Casablanca&remote=true&limit=20
```

### API de matching — Flask, port 5001

| Methode | Route | Description |
|---|---|---|
| `GET` | `/api/health` | Etat du moteur et nombre d'offres chargees |
| `GET` | `/api/filters` | Valeurs disponibles pour les filtres |
| `POST` | `/api/match` | Classe les offres selon un profil |
| `POST` | `/api/retrain` | Relance l'entrainement du modele |

Exemple de corps JSON pour `POST /api/match` :

```json
{
  "title": "Data Analyst",
  "city": "Casablanca",
  "country": "Morocco",
  "work_mode": "remote",
  "skills": ["python", "sql"],
  "top_n": 20
}
```

## Interface web

L'application React est dans `frontend/`. Elle propose une recherche par poste, ville et mode de travail, puis affiche les recommandations renvoyees par l'API Flask. Les parametres de requete de l'interface sont actuellement definis dans `frontend/src/App.tsx`.


## Contribution

1. Creez une branche a partir de `main` pour chaque changement.
2. Gardez les modifications ciblees et ajoutez ou mettez a jour les tests et la documentation associes.
3. Avant d'ouvrir une pull request, lancez les verifications concernees (par exemple `npm run build` et `npm run lint` depuis `frontend/`).
4. N'incluez jamais de secrets, de fichiers `.env`, d'exports de donnees ou de modeles entraines dans une pull request.
