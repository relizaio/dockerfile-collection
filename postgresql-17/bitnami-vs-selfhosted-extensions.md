# Extension Differences

## Missing Extensions

**Critical:**
- PostGIS - Geospatial functionality
- pgvector - AI/ML vector embeddings  
- pgaudit - Audit logging

**Standard:**
- orafce - Oracle compatibility
- pljava - Java procedures
- wal2json - Logical replication
- ODBC drivers

## Included Extensions

All PostgreSQL contrib extensions:
- amcheck, bloom, btree_gin, btree_gist, citext, cube, fuzzystrmatch
- hstore, isn, ltree, pg_trgm, pgcrypto, pg_stat_statements, uuid-ossp
- file_fdw, postgres_fdw, sslinfo, unaccent

## Build Instructions

**pgaudit:**
```dockerfile
RUN git clone --depth 1 --branch REL_17_STABLE https://github.com/pgaudit/pgaudit.git /tmp/pgaudit && \
    cd /tmp/pgaudit && make USE_PGXS=1 && make USE_PGXS=1 install
```

**pgvector:**
```dockerfile
RUN git clone --depth 1 --branch v0.7.4 https://github.com/pgvector/pgvector.git /tmp/pgvector && \
    cd /tmp/pgvector && make && make install
```

**PostGIS:**
```dockerfile
RUN apt-get update && apt-get install -y postgresql-17-postgis-3 postgresql-17-postgis-3-scripts
```
