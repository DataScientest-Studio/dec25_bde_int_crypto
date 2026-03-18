# ML Pipeline

Stack ML autonome pour la collecte de données Binance, l'entraînement et la prédiction.

## Architecture
```
ml-pipeline/
├── docker-compose.yml        # Orchestre les 5 services
├── .env                      # Variables d'environnement
├── .env.example              # Template à commiter
├── dockerfiles/
│   ├── Dockerfile.collector  # Collecte Binance → MongoDB + CSV
│   ├── Dockerfile.trainer    # CSV → entraînement → .pkl
│   └── Dockerfile.api        # FastAPI → /predict
└── README.md
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| mongodb-ml | 27018 | Base de données ML dédiée |
| mongo-express-ml | 8083 | Interface web MongoDB |
| binance-collector | - | Collecte données Binance (job ponctuel) |
| model-trainer | - | Entraîne le modèle (job ponctuel) |
| prediction-api | 8001 | FastAPI · POST /predict |

## Volumes

| Volume | Description |
|--------|-------------|
| `ml_pipeline_mongodb_ml_data` | Données MongoDB |
| `ml_pipeline_collector_data` | CSV raw + processed |
| `ml_pipeline_model_artifacts` | Fichiers .pkl |

## Démarrage rapide
```bash
cd ml-pipeline/

# 1. Copier et configurer les variables
cp .env.example .env

# 2. Démarrer MongoDB
docker compose up -d mongodb-ml

# 3. Lancer le collector (one-shot)
docker compose run --rm binance-collector

# 4. Entraîner le modèle (one-shot)
docker compose run --rm model-trainer

# 5. Démarrer l'API de prédiction
docker compose up -d prediction-api

# 6. Démarrer Mongo Express (optionnel)
docker compose up -d mongo-express-ml
```

## Endpoints API

| Méthode | URL | Description |
|---------|-----|-------------|
| GET | http://localhost:8001/ | Health check |
| GET | http://localhost:8001/predict/logistic/status | Statut du modèle |
| POST | http://localhost:8001/predict/logistic/ | Prédiction depuis CSV |
| GET | http://localhost:8001/docs | Swagger UI |

## Variables d'environnement

| Variable | Défaut | Description |
|----------|--------|-------------|
| MONGODB_URI | mongodb://admin:password@mongodb-ml:27017/ | URI MongoDB |
| MONGODB_DATABASE | crypto_data | Nom de la base |
| BINANCE_SYMBOL | BTCUSDT | Paire de trading |
| BINANCE_INTERVAL | 5m | Intervalle des bougies |
| BINANCE_START_DATE | 2024-01-01 | Date de début collecte |
| DATA_DIR | /app/data | Dossier des données |
| MODEL_PATH | /app/models/logistic_regression_model.pkl | Chemin du modèle |
| SCALER_PATH | /app/models/logistic_regression_scaler.pkl | Chemin du scaler |

## Réentraînement

Pour réentraîner le modèle avec de nouvelles données :
```bash
# 1. Relancer le collector pour fetcher les nouvelles données
docker compose run --rm binance-collector

# 2. Réentraîner
docker compose run --rm model-trainer

# 3. Redémarrer l'API pour charger le nouveau .pkl
docker compose restart prediction-api
```

## Docker Hub
```bash
# Build toutes les images
docker compose build

# Tag et push
docker tag ml_pipeline-binance-collector monuser/binance-collector:v1.0
docker tag ml_pipeline-model-trainer monuser/model-trainer:v1.0
docker tag ml_pipeline-prediction-api monuser/prediction-api:v1.0

docker push monuser/binance-collector:v1.0
docker push monuser/model-trainer:v1.0
docker push monuser/prediction-api:v1.0
```
