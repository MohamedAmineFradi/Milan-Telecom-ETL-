# Docker Setup for Milan Telecom ETL

## Overview
This Docker setup containerizes the ETL application with a PostgreSQL+PostGIS database.

## Structure
- **Dockerfile**: Builds the ETL application image
- **docker-compose.yml**: Orchestrates PostgreSQL and ETL services
- **.dockerignore**: Excludes unnecessary files from Docker build

## Prerequisites
- Docker Engine 20.10+
- Docker Compose 2.0+
- Data files in `data_milan_cdr_kaggle/` directory

## Quick Start

### 1. Create environment file (optional)
```bash
cp .env.example .env
# Edit .env if you want custom database credentials
```

### 2. Build and run
```bash
docker-compose up --build
```

This will:
- Build the ETL application image
- Start PostgreSQL with PostGIS extension
- Wait for database to be healthy
- Run the ETL pipeline

### 3. View logs
```bash
# All services
docker-compose logs -f

# Just ETL application
docker-compose logs -f etl

# Just PostgreSQL
docker-compose logs -f postgres
```

## Usage

### Run the full ETL pipeline
```bash
docker-compose up
```

### Run only PostgreSQL (for interactive work)
```bash
docker-compose up postgres
```

### Run ETL with custom arguments
```bash
docker-compose run etl python main.py --limit-files 3
```

### Connect to PostgreSQL from host
```bash
psql -h localhost -U postgres -d milan_telecom
```

### Stop and remove all
```bash
docker-compose down
```

### Remove data volumes (careful!)
```bash
docker-compose down -v
```

## Configuration

Edit environment variables in `docker-compose.yml` or create a `.env` file:

```env
DB_NAME=milan_telecom
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=postgres
DB_PORT=5432
```

## Data Volumes
- **postgres_data**: PostgreSQL data (persists between restarts)
- **data_milan_cdr_kaggle**: Read-only mount of source data

## Troubleshooting

### Database connection failed
Check database health:
```bash
docker-compose ps
docker-compose logs postgres
```

### Out of disk space
Clean up Docker resources:
```bash
docker-compose down -v
docker system prune
```

### Rebuild image
```bash
docker-compose build --no-cache
```
