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
    if ! docker run -d --name $BITNAMI_CONTAINER \
        -e POSTGRESQL_USERNAME=$DB_USER \
        -e POSTGRESQL_PASSWORD=$DB_PASSWORD \
        -e POSTGRESQL_DATABASE=$DB_NAME \
        -v $VOLUME_NAME:/bitnami/postgresql \
        -p 15432:5432 $BITNAMI_IMAGE >/dev/null; then
        echo "  ✗ Failed to start Bitnami container"
        return 1
    fi
    
    for i in {1..30}; do
        docker exec $BITNAMI_CONTAINER pg_isready -U postgres -d postgres >/dev/null 2>&1 && break
        sleep 2
        if [[ $i -eq 30 ]]; then
            echo "  ✗ Bitnami PostgreSQL failed to start"
            docker logs $BITNAMI_CONTAINER
            return 1
        fi
    done
    
    # Wait for user creation to complete
    sleep 5
    
    # Verify container is actually healthy
    local container_status=$(docker inspect --format='{{.State.Status}}' $BITNAMI_CONTAINER)
    [[ "$container_status" == "running" ]] || { echo "  ✗ Bitnami container not running: $container_status"; return 1; }
    
    # Test basic connection with created user
    docker exec -e PGPASSWORD=$DB_PASSWORD $BITNAMI_CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT 1;" >/dev/null || { echo "  ✗ Bitnami connection test failed"; return 1; }
    
    echo "  ✓ Bitnami ready and verified"
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
    if ! docker run -d --name $SELFHOSTED_CONTAINER \
        -e POSTGRESQL_USERNAME=$DB_USER \
        -e POSTGRESQL_PASSWORD=$DB_PASSWORD \
        -e POSTGRESQL_DATABASE=$DB_NAME \
        -v $VOLUME_NAME:/relizaio/postgresql \
        -p 15433:5432 $SELFHOSTED_IMAGE >/dev/null; then
        echo "  ✗ Failed to start self-hosted container"
        return 1
    fi
    
    for i in {1..30}; do
        docker exec $SELFHOSTED_CONTAINER pg_isready -U $DB_USER -d $DB_NAME >/dev/null 2>&1 && break
        sleep 2
        if [[ $i -eq 30 ]]; then
            echo "  ✗ Self-hosted PostgreSQL failed to start"
            docker logs $SELFHOSTED_CONTAINER
            return 1
        fi
    done
    
    # Verify container is actually healthy
    local container_status=$(docker inspect --format='{{.State.Status}}' $SELFHOSTED_CONTAINER)
    [[ "$container_status" == "running" ]] || { echo "  ✗ Self-hosted container not running: $container_status"; return 1; }
    
    # Test basic connection
    docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT 1;" >/dev/null || { echo "  ✗ Self-hosted connection test failed"; return 1; }
    
    sleep 3
    echo "  ✓ Self-hosted ready and verified"
}

validate_data() {
    echo "• Validating data integrity..."
    
    # Check table exists
    local table_exists=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_table';" | xargs)
    [[ "$table_exists" == "1" ]] || { echo "  ✗ Table test_table does not exist (found: $table_exists)"; return 1; }
    
    # Check exact row count
    local count=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM test_table;" | xargs)
    [[ "$count" == "3" ]] || { echo "  ✗ Data count mismatch: expected 3, got $count"; return 1; }
    
    # Check specific data values
    local test1_value=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT value FROM test_table WHERE name = 'test1';" | xargs)
    [[ "$test1_value" == "100" ]] || { echo "  ✗ Data integrity failed: test1 value expected 100, got $test1_value"; return 1; }
    
    # Check index exists
    local index_exists=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_test_name';" | xargs)
    [[ "$index_exists" == "1" ]] || { echo "  ✗ Index idx_test_name missing (found: $index_exists)"; return 1; }
    
    echo "  ✓ Data integrity verified (3 rows, correct values, index present)"
}

test_queries() {
    echo "• Testing queries..."
    
    # Test transaction
    docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -c "
        BEGIN;
        INSERT INTO test_table (name, value) VALUES ('test4', 400);
        COMMIT;
    " >/dev/null
    
    # Verify transaction worked
    local count=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM test_table;" | xargs)
    [[ "$count" == "4" ]] || { echo "  ✗ Transaction failed: expected 4 rows, got $count"; return 1; }
    
    # Test SELECT with WHERE clause
    local test4_value=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT value FROM test_table WHERE name = 'test4';" | xargs)
    [[ "$test4_value" == "400" ]] || { echo "  ✗ SELECT query failed: expected 400, got $test4_value"; return 1; }
    
    # Test aggregate function
    local sum_value=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT SUM(value) FROM test_table;" | xargs)
    [[ "$sum_value" == "1000" ]] || { echo "  ✗ Aggregate query failed: expected 1000, got $sum_value"; return 1; }
    
    echo "  ✓ Queries working (transaction, SELECT, aggregate)"
}

check_version() {
    echo "• Checking version..."
    local version=$(docker exec -e PGPASSWORD=$DB_PASSWORD $SELFHOSTED_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT version();" | head -1 | xargs)
    
    # Verify it's PostgreSQL 17
    [[ "$version" == *"PostgreSQL 17"* ]] || { echo "  ✗ Wrong PostgreSQL version: $version"; return 1; }
    
    # Verify it's Debian 13 build
    [[ "$version" == *"pgdg13"* ]] || { echo "  ✗ Not Debian 13 build: $version"; return 1; }
    
    echo "  ✓ PostgreSQL 17 on Debian 13 confirmed"
    echo "    $version"
}

main() {
    echo "• Building image..."
    if ! docker build -t $SELFHOSTED_IMAGE ./debian-12/ >/dev/null 2>&1; then
        echo "  ✗ Image build failed"
        echo "Build output:"
        docker build -t $SELFHOSTED_IMAGE ./debian-12/
        return 1
    fi
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
