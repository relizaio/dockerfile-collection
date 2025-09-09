[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![PostgreSQL Version](https://img.shields.io/badge/PostgreSQL-17.6-blue.svg)](https://www.postgresql.org/)
[![Debian Version](https://img.shields.io/badge/Debian-12.12-blue.svg)](https://www.debian.org/)

# PostgreSQL 17 - Self-Hosted Container

Production-ready PostgreSQL 17.6 container image with Bitnami compatibility.

## Quick Start

```bash
docker run -d --name postgresql \
  -e POSTGRESQL_ROOT_PASSWORD=secretpassword \
  -e POSTGRESQL_DATABASE=mydb \
  -e POSTGRESQL_USERNAME=myuser \
  -e POSTGRESQL_PASSWORD=mypassword \
  -v postgresql_data:/relizaio/postgresql \
  -p 5432:5432 \
  postgresql-17-selfhosted
```

## Features

- PostgreSQL 17.6 on Debian 12
- Non-root execution (UID 1001)
- Bitnami compatibility (volumes, environment variables)
- TLS support and replication ready
- Optimized build (5 layers, smaller size)

## Building

```bash
cd debian-12
docker build -t postgresql-17-selfhosted .
```

## Missing Extensions

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

Licensed under AGPL-3.0-only.

---

**Built by Reliza Incorporated**
