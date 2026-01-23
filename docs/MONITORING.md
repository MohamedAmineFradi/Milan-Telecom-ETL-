# PostgreSQL Monitoring Setup

This project includes a complete monitoring stack for PostgreSQL database using Grafana, Prometheus, and Postgres Exporter.

## Components

- **Grafana**: Visualization and dashboarding (Port 3000)
- **Prometheus**: Metrics collection and storage (Port 9090)
- **Postgres Exporter**: PostgreSQL metrics exporter (Port 9187)

## Quick Start

1. Start all services:
```bash
docker-compose up -d
```

2. Access Grafana:
   - URL: http://localhost:3000
   - Default credentials: admin/admin (change on first login)
   - Pre-configured PostgreSQL dashboard will be available

3. Access Prometheus (optional):
   - URL: http://localhost:9090
   - Check targets: http://localhost:9090/targets

## Available Metrics

The PostgreSQL dashboard displays:

### Connection Metrics
- Active connections count
- Connection usage percentage
- Max connections limit

### Performance Metrics
- Transaction rate (commits/rollbacks)
- Query execution statistics
- Tuples changed (inserts/updates/deletes)

### Cache & I/O
- Cache hit ratio
- Cache hits vs disk reads
- Block read/write statistics

### Database Size
- Total database size over time
- Growth rate

## Custom Queries

To add custom metrics, edit the Postgres Exporter queries:

1. Create a custom queries file
2. Mount it in the postgres-exporter service
3. Add the metric to Grafana dashboard

Example custom query:
```yaml
pg_table_count:
  query: "SELECT schemaname, COUNT(*) FROM pg_tables GROUP BY schemaname"
  metrics:
    - schemaname:
        usage: "LABEL"
        description: "Schema name"
    - count:
        usage: "GAUGE"
        description: "Number of tables"
```

## Environment Variables

Configure in `.env` file or set directly:

```bash
# Database
DB_NAME=milan_telecom
DB_USER=postgres
DB_PASSWORD=postgres
DB_PORT=5432

# Grafana
GRAFANA_USER=admin
GRAFANA_PASSWORD=admin
```

## Alerting (Optional)

To set up alerts in Grafana:

1. Go to Alerting → Alert rules
2. Create new alert rule
3. Example conditions:
   - High connection usage (>80%)
   - Low cache hit ratio (<90%)
   - Database size growth rate

## Data Retention

- **Prometheus**: Default 15 days (configurable in prometheus.yml)
- **Grafana**: Persistent storage via Docker volume

## Troubleshooting

### No data in Grafana
1. Check Prometheus targets: http://localhost:9090/targets
2. Verify postgres-exporter is running: `docker logs milan_postgres_exporter`
3. Check datasource connection in Grafana
