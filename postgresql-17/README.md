[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL Version](https://img.shields.io/badge/PostgreSQL-17.6-blue.svg)](https://www.postgresql.org/)
[![Debian Version](https://img.shields.io/badge/Debian-13.1-blue.svg)](https://www.debian.org/)

# PostgreSQL 17 Self-Hosted Container

Self-hosted PostgreSQL 17 container with full control and Bitnami compatibility.

## Available Images

- **Core**: `debian-13/Dockerfile` - PostgreSQL 17.6 production ready
- **pgAudit**: `debian-13/Dockerfile.pgaudit` - PostgreSQL 17.6 + audit extension

## Quick Start

```bash
docker run -d --name postgresql \
  -e POSTGRESQL_POSTGRES_PASSWORD=secretpassword \
  -e POSTGRESQL_DATABASE=mydb \
  -e POSTGRESQL_USERNAME=myuser \
  -e POSTGRESQL_PASSWORD=mypassword \
  -v postgresql_data:/relizaio/postgresql \
  -p 5432:5432 \
  your-registry/postgresql:17.6
```

## Table of Contents

- [Authentication](#authentication)
- [Configuration](#configuration)
- [Database Initialization](#initdb)
- [Pre-initialization Scripts](#pre-init)
- [Resource Presets](#resource-presets)
- [Monitoring](#monitoring)
- [pgAudit Extension](#pgaudit-extension)

## Authentication

### Password Configuration
```bash
# PostgreSQL superuser password (required)
POSTGRESQL_POSTGRES_PASSWORD=your_secure_password

# Custom user password (if creating custom user)
POSTGRESQL_PASSWORD=user_password
```

### Password Sources
```bash
# Load passwords from files (Docker secrets)
POSTGRESQL_POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password
POSTGRESQL_PASSWORD_FILE=/run/secrets/user_password

# Allow empty passwords (development only)
ALLOW_EMPTY_PASSWORD=yes
```

### User Management
```bash
# Create a custom user with database
POSTGRESQL_USERNAME=myapp
POSTGRESQL_PASSWORD=myapp_password
POSTGRESQL_DATABASE=myapp_db
```

### Connection Limits
```bash
# Limit connections per user
POSTGRESQL_USERNAME_CONNECTION_LIMIT=100
POSTGRESQL_POSTGRES_CONNECTION_LIMIT=50
```

### LDAP Authentication

```bash
POSTGRESQL_ENABLE_LDAP=yes
POSTGRESQL_LDAP_URL="ldap://ldap.example.com"
POSTGRESQL_LDAP_PREFIX="cn="
POSTGRESQL_LDAP_SUFFIX=",dc=example,dc=com"
```

### TLS Configuration

```bash
POSTGRESQL_ENABLE_TLS=yes
POSTGRESQL_TLS_CERT_FILE=/path/to/server.crt
POSTGRESQL_TLS_KEY_FILE=/path/to/server.key
```

## Configuration

### Core Settings

```bash
POSTGRESQL_PORT_NUMBER=5432
POSTGRESQL_MAX_CONNECTIONS=100
POSTGRESQL_STATEMENT_TIMEOUT=0
```

### Logging

```bash
POSTGRESQL_LOG_CONNECTIONS=on
POSTGRESQL_LOG_LINE_PREFIX="%t [%p]: user=%u,db=%d "
```

### Replication

```bash
POSTGRESQL_WAL_LEVEL=replica
POSTGRESQL_REPLICATION_MODE=master
```

### Other Settings

```bash
POSTGRESQL_PASSWORD_ENCRYPTION=scram-sha-256
POSTGRESQL_TIMEZONE="UTC"
```

### Custom Configuration

```bash
-v /path/to/postgresql.conf:/opt/relizaio/postgresql/conf/postgresql.conf
-v /path/to/pg_hba.conf:/opt/relizaio/postgresql/conf/pg_hba.conf
```

## Initdb

```bash
POSTGRESQL_INITDB_ARGS="--encoding=UTF8 --locale=en_US.UTF-8"
POSTGRESQL_INITDB_WAL_DIR=/custom/wal/path
POSTGRESQL_INIT_MAX_TIMEOUT=60
```

### Initialization Scripts

```bash
-v /path/to/init/scripts:/docker-entrypoint-initdb.d
```

Supported: `.sh`, `.sql`, `.sql.gz` (executed alphabetically)

## Pre-init

```bash
-v /path/to/preinit/scripts:/docker-entrypoint-preinitdb.d
```

Runs before PostgreSQL initialization for system-level setup.

## Resource Presets

| Size | Connections | Shared Buffers | Cache Size |
|------|-------------|----------------|------------|
| nano | 20 | 32MB | 128MB |
| micro | 50 | 64MB | 256MB |
| small | 100 | 128MB | 512MB |
| medium | 200 | 256MB | 1GB |
| large | 400 | 512MB | 2GB |
| xlarge | 800 | 1GB | 4GB |
| 2xlarge | 1000 | 2GB | 8GB |

```bash
# Apply preset
POSTGRESQL_PRESET=medium

# Or manual
POSTGRESQL_MAX_CONNECTIONS=200
POSTGRESQL_SHARED_BUFFERS=256MB
```

## Monitoring

```bash
POSTGRESQL_ENABLE_METRICS=yes
POSTGRESQL_METRICS_PORT=9187
```

Metrics available at `http://localhost:9187/metrics`

## pgAudit Extension

```bash
# Build pgAudit image
docker build -t postgresql-audit:17.6 -f Dockerfile.pgaudit .

# Run with audit logging
docker run -d \
  -e POSTGRESQL_POSTGRES_PASSWORD=password \
  -e POSTGRESQL_PGAUDIT_LOG=all \
  postgresql-audit:17.6

# Enable extension
psql -U postgres -c "CREATE EXTENSION pgaudit;"
```

### Audit Variables
- `POSTGRESQL_PGAUDIT_LOG`: `all`, `read`, `write`, `ddl`
- `POSTGRESQL_PGAUDIT_LOG_LEVEL`: `log`, `notice`, `warning`

## Replication

### Master
```bash
POSTGRESQL_REPLICATION_MODE=master
POSTGRESQL_REPLICATION_USER=replicator
POSTGRESQL_REPLICATION_PASSWORD=password
```

### Replica
```bash
POSTGRESQL_REPLICATION_MODE=slave
POSTGRESQL_MASTER_HOST=postgresql-master
POSTGRESQL_REPLICATION_USER=replicator
POSTGRESQL_REPLICATION_PASSWORD=password
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRESQL_POSTGRES_PASSWORD` | - | Superuser password (required) |
| `POSTGRESQL_USERNAME` | - | Custom user |
| `POSTGRESQL_PASSWORD` | - | Custom user password |
| `POSTGRESQL_DATABASE` | `postgres` | Database name |
| `POSTGRESQL_MAX_CONNECTIONS` | `100` | Max connections |
| `POSTGRESQL_REPLICATION_MODE` | `master` | `master`/`slave` |
| `POSTGRESQL_ENABLE_TLS` | `no` | Enable TLS |
| `ALLOW_EMPTY_PASSWORD` | `no` | Allow empty passwords |

## Troubleshooting

```bash
# Check logs
docker logs postgresql-container

# Check status
docker exec postgresql-container pg_isready -U postgres

# Fix permissions
sudo chown -R 1001:1001 /path/to/data

# Debug mode
docker run -e RELIZAIO_DEBUG=true postgresql:17.6
```

## Migration from Bitnami

| Bitnami | Self-hosted |
|---------|-------------|
| `/bitnami/postgresql` | `/relizaio/postgresql` |
| `POSTGRES_PASSWORD` | `POSTGRESQL_POSTGRES_PASSWORD` |
| `POSTGRES_USER` | `POSTGRESQL_USERNAME` |

```bash
# Backup
docker exec bitnami-postgres pg_dumpall -U postgres > backup.sql

# Migrate
cp -r /bitnami/postgresql /relizaio/postgresql

# Start
docker run -d -v /relizaio/postgresql:/relizaio/postgresql postgresql:17.6
```

## License

MIT License - see [LICENSE](LICENSE) file.

## Support

- [GitHub Issues](https://github.com/relizaio/dockerfile-collection/issues)
- [Documentation](https://github.com/relizaio/dockerfile-collection/tree/master/postgresql-17)

See [bitnami-vs-selfhosted-extensions.md](bitnami-vs-selfhosted-extensions.md) for details on missing extensions like PostGIS, pgvector, and pgaudit.

## Testing

```bash
# Test data migration from Bitnami
./test-data-migration.sh run
```

## Configuration

### Environment Variables

- `POSTGRESQL_ROOT_PASSWORD` - Required
- `POSTGRESQL_DATABASE` - Database name
- `POSTGRESQL_USERNAME` - User name  
- `POSTGRESQL_PASSWORD` - User password
- `POSTGRESQL_MAX_CONNECTIONS` - Connection limit
- `RELIZAIO_DEBUG` - Debug mode

### Volume Mounts

- `/relizaio/postgresql` - Data directory
- `/bitnami/postgresql` - Bitnami compatibility

## License

Licensed under MIT.

---

**Built by Reliza Incorporated**
