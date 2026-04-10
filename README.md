# Kit Projet — Formation Airflow IPSSI

## Prérequis

- Docker Desktop installé et lancé
- Au moins **4 Go de RAM** alloués à Docker
- Ports libres : 8080 (Airflow), 9000/9001 (MinIO), 5000 (API), 5432/5433/5434 (PostgreSQL)

## Lancement

```bash
# Lancer tous les services
docker compose up -d

# Attendre ~1 minute que tout soit pret
docker compose ps
```

## Accès aux services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | <http://localhost:8080> | `admin` / `admin` |
| **MinIO Console** | <http://localhost:9001> | `minio_admin` / `minio_password_2026` |
| **API simulée** | <http://localhost:5000/health> | Bearer `formation-token-2026` |
| **PostgreSQL DWH** | `localhost:5433` | `dwh_user` / `dwh_password` / db `dwh` |
| **PostgreSQL Source** | `localhost:5434` | `source_user` / `source_password` / db `source_db` |

## Endpoints de l'API simulée

Toutes les requêtes nécessitent le header `Authorization: Bearer formation-token-2026`.

```bash
# Commandes
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/orders?date=2026-04-07"
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/customers?limit=10"
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/products"
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/metrics?date=2026-04-07&metric_type=page_views"
```

Les données sont **déterministes** : la même date retourne toujours les mêmes résultats.

## Connections Airflow pré-configurées

Les Connections sont configurées via variables d'environnement dans le docker-compose :

| conn_id | Service |
|---------|---------|
| `postgres_dwh` | PostgreSQL DWH (schémas staging, dwh, analytics) |
| `postgres_source` | PostgreSQL Source DB (sujet B) |
| `minio_local` | MinIO S3-compatible (bucket `data-lake`) |
| `api_ecommerce` | API Flask simulée |

## Structure du projet

```text
kit-projet/
|-- docker-compose.yaml    # Airflow + PostgreSQL + MinIO + API
|-- .env                   # Variables d'environnement
|-- dags/                  # Vos DAGs ici
|-- plugins/
|   |-- hooks/             # Vos Custom Hooks ici
|   |-- operators/         # Vos Custom Operators ici
|-- tests/                 # Tests pytest
|-- api/                   # API Flask simulee
|-- init-db/               # Scripts SQL d'initialisation
|-- init-minio/            # Script de seed MinIO
```

## Lancer les tests

```bash
docker compose exec airflow-worker pytest tests/ -v
```

## Sujets de mini-projets

- **Sujet A** — Pipeline ELT e-commerce (extraction API → MinIO → PostgreSQL → qualité)
- **Sujet B** — Orchestration multi-sources analytics (4 sources hétérogènes → agrégation)

Voir le plan de formation pour les cahiers des charges complets.

## Commandes utiles

```bash
# Voir les logs d'un service
docker compose logs airflow-scheduler --tail=30

# Se connecter au DWH
docker compose exec postgres-dwh psql -U dwh_user -d dwh

# Se connecter a la source DB
docker compose exec postgres-source psql -U source_user -d source_db

# Arreter tout
docker compose down

# Arreter et supprimer les volumes (reset complet)
docker compose down -v
```
