#!/bin/bash
set -e

# Configuration
BITNAMI_IMAGE="bitnami/postgresql:17.5.0-debian-12-r4"
SELFHOSTED_IMAGE="postgresql-17-selfhosted"
VOLUME_NAME="pg_test_data"
BITNAMI_CONTAINER="pg-bitnami"
SELFHOSTED_CONTAINER="pg-selfhosted"
DB_USER="testuser"
DB_PASSWORD="testpass123"
DB_NAME="testdb"

echo "=== PostgreSQL Migration Test ==="
echo "Bitnami -> Self-hosted"
echo

cleanup() {
    docker stop $BITNAMI_CONTAINER $SELFHOSTED_CONTAINER >/dev/null 2>&1 || true
    docker rm $BITNAMI_CONTAINER $SELFHOSTED_CONTAINER >/dev/null 2>&1 || true
    docker volume rm $VOLUME_NAME >/dev/null 2>&1 || true
}
trap cleanup EXIT

setup_bitnami() {
    echo "• Setting up Bitnami PostgreSQL..."
    docker volume create $VOLUME_NAME >/dev/null
    docker run -d --name $BITNAMI_CONTAINER \
        -e POSTGRESQL_USERNAME=$DB_USER \
        -e POSTGRESQL_PASSWORD=$DB_PASSWORD \
        -e POSTGRESQL_DATABASE=$DB_NAME \
        -v $VOLUME_NAME:/bitnami/postgresql \
        -p 15432:5432 $BITNAMI_IMAGE >/dev/null
    
    for i in {1..30}; do
        docker exec $BITNAMI_CONTAINER pg_isready -U $DB_USER -d $DB_NAME >/dev/null 2>&1 && break
        sleep 2
    done
    sleep 3
    echo "  ✓ Bitnami ready"
}

populate_data() {
    echo "• Populating test data..."
    docker exec -e PGPASSWORD=$DB_PASSWORD $BITNAMI_CONTAINER psql -U $DB_USER -d $DB_NAME -c "
        CREATE TABLE test_table (id SERIAL PRIMARY KEY, name VARCHAR(50), value INTEGER);
        INSERT INTO test_table (name, value) VALUES ('test1', 100), ('test2', 200), ('test3', 300);
        CREATE INDEX idx_test_name ON test_table(name);
    " >/dev/null
    echo "  ✓ Test data created"
}

migrate_to_selfhosted() {
    echo "• Migrating to self-hosted..."
    docker stop $BITNAMI_CONTAINER >/dev/null
    docker run -d --name $SELFHOSTED_CONTAINER \
        -e POSTGRESQL_USERNAME=$DB_USER \
        -e POSTGRESQL_PASSWORD=$DB_PASSWORD \
        -e POSTGRESQL_DATABASE=$DB_NAME \
        -v $VOLUME_NAME:/relizaio/postgresql \
        -p 15433:5432 $SELFHOSTED_IMAGE >/dev/null
    
    for i in {1..30}; do
        docker exec $SELFHOSTED_CONTAINER pg_isready -U $DB_USER -d $DB_NAME >/dev/null 2>&1 && break
        sleep 2
    done
    sleep 3
    echo "  ✓ Self-hosted ready"
}

validate_data() {
    echo "• Validating data integrity..."
    local count=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM test_table;" | xargs)
    [[ "$count" == "3" ]] || { echo "  ✗ Data validation failed"; return 1; }
    
    local indexes=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public';" | xargs)
    [[ "$indexes" -ge "1" ]] || { echo "  ✗ Index validation failed"; return 1; }
    echo "  ✓ Data integrity verified"
}

test_queries() {
    echo "• Testing queries..."
    docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -c "
        BEGIN;
        INSERT INTO test_table (name, value) VALUES ('test4', 400);
        COMMIT;
    " >/dev/null
    local count=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM test_table;" | xargs)
    [[ "$count" == "4" ]] || { echo "  ✗ Query test failed"; return 1; }
    echo "  ✓ Queries working"
}

check_version() {
    echo "• Checking version..."
    local version=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT version();" | head -1 | xargs)
    echo "  ✓ $version"
}

main() {
    echo "• Building image..."
    docker build -t $SELFHOSTED_IMAGE ./debian-12/ >/dev/null
    echo "  ✓ Image built"
    echo
    
    setup_bitnami
    populate_data
    migrate_to_selfhosted
    validate_data
    test_queries
    check_version
    
    echo
    echo "=== Migration Test Completed Successfully ==="
}

[[ "$1" == "run" ]] && main || echo "Usage: $0 run"
