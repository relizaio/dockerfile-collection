#!/bin/bash
set -e

# Configuration
BITNAMI_IMAGE="bitnami/postgresql:17.5.0-debian-12-r4"
ORIGINAL_BITNAMI_IMAGE="bitnami/postgresql:17"
SELFHOSTED_IMAGE="postgresql-17-selfhosted"
VOLUME_NAME="pg_test_data"
BITNAMI_CONTAINER="pg-bitnami"
ORIGINAL_BITNAMI_CONTAINER="pg-original-bitnami"
SELFHOSTED_CONTAINER="pg-selfhosted"
DB_USER="testuser"
DB_PASSWORD="testpass123"
DB_NAME="testdb"

echo "=== PostgreSQL Migration & Compatibility Test ==="
echo "Testing: Original Bitnami vs Self-hosted vs Legacy Bitnami"
echo

cleanup() {
    docker stop $BITNAMI_CONTAINER $ORIGINAL_BITNAMI_CONTAINER $SELFHOSTED_CONTAINER >/dev/null 2>&1 || true
    docker rm $BITNAMI_CONTAINER $ORIGINAL_BITNAMI_CONTAINER $SELFHOSTED_CONTAINER >/dev/null 2>&1 || true
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

test_original_bitnami_api() {
    echo "• Testing Original Bitnami API compatibility..."
    docker volume create ${VOLUME_NAME}_orig >/dev/null
    
    # Test original Bitnami with same environment variables
    if ! docker run -d --name $ORIGINAL_BITNAMI_CONTAINER \
        -e POSTGRESQL_USERNAME=$DB_USER \
        -e POSTGRESQL_PASSWORD=$DB_PASSWORD \
        -e POSTGRESQL_DATABASE=$DB_NAME \
        -v ${VOLUME_NAME}_orig:/bitnami/postgresql \
        -p 15434:5432 $ORIGINAL_BITNAMI_IMAGE >/dev/null; then
        echo "  ✗ Failed to start Original Bitnami container"
        return 1
    fi
    
    # Wait for startup
    for i in {1..30}; do
        docker exec $ORIGINAL_BITNAMI_CONTAINER pg_isready -U postgres -d postgres >/dev/null 2>&1 && break
        sleep 2
        if [[ $i -eq 30 ]]; then
            echo "  ✗ Original Bitnami PostgreSQL failed to start"
            docker logs $ORIGINAL_BITNAMI_CONTAINER
            return 1
        fi
    done
    sleep 5
    
    # Test connection and basic operations
    docker exec -e PGPASSWORD=$DB_PASSWORD $ORIGINAL_BITNAMI_CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT 1;" >/dev/null || { echo "  ✗ Original Bitnami connection test failed"; return 1; }
    
    # Test persistent storage behavior
    docker exec -e PGPASSWORD=$DB_PASSWORD $ORIGINAL_BITNAMI_CONTAINER psql -U $DB_USER -d $DB_NAME -c "CREATE TABLE orig_test (id INT, name TEXT);" >/dev/null
    docker exec -e PGPASSWORD=$DB_PASSWORD $ORIGINAL_BITNAMI_CONTAINER psql -U $DB_USER -d $DB_NAME -c "INSERT INTO orig_test VALUES (1, 'test');" >/dev/null
    
    # Restart container to test persistent storage
    docker stop $ORIGINAL_BITNAMI_CONTAINER >/dev/null
    docker start $ORIGINAL_BITNAMI_CONTAINER >/dev/null
    sleep 10
    
    # Verify data persisted and connection works after restart
    local count=$(docker exec -e PGPASSWORD=$DB_PASSWORD $ORIGINAL_BITNAMI_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM orig_test;" 2>/dev/null | xargs || echo "0")
    if [[ "$count" == "1" ]]; then
        echo "  ✓ Original Bitnami persistent storage works correctly"
    else
        echo "  ✗ Original Bitnami persistent storage failed (count: $count)"
        return 1
    fi
    
    docker stop $ORIGINAL_BITNAMI_CONTAINER >/dev/null
    docker rm $ORIGINAL_BITNAMI_CONTAINER >/dev/null
    docker volume rm ${VOLUME_NAME}_orig >/dev/null
}

compare_api_compatibility() {
    echo "• Comparing Bitnami API compatibility..."
    
    # Test 1: POSTGRESQL_* vs POSTGRES_* variable aliasing
    echo "  Testing POSTGRESQL_* vs POSTGRES_* variable aliasing..."
    
    docker volume create api_test_alias_bitnami >/dev/null 2>&1
    docker volume create api_test_alias_selfhosted >/dev/null 2>&1
    
    # Test Bitnami with POSTGRES_* variables
    docker run -d --name api_alias_bitnami \
        -e POSTGRES_USER=aliasuser \
        -e POSTGRES_PASSWORD=aliaspass123 \
        -e POSTGRES_DB=aliasdb \
        -v api_test_alias_bitnami:/bitnami/postgresql \
        $ORIGINAL_BITNAMI_IMAGE >/dev/null 2>&1
    
    sleep 15
    docker exec api_alias_bitnami bash -c 'until pg_isready -U aliasuser; do sleep 1; done' >/dev/null 2>&1
    local bitnami_alias_test=$(docker exec api_alias_bitnami bash -c 'PGPASSWORD=aliaspass123 psql -U aliasuser -d aliasdb -c "SELECT current_database();" >/dev/null 2>&1' && echo "SUCCESS" || echo "FAILED")
    docker stop api_alias_bitnami >/dev/null 2>&1 && docker rm api_alias_bitnami >/dev/null 2>&1
    
    # Test Self-hosted with POSTGRES_* variables
    docker run -d --name api_alias_selfhosted \
        -e POSTGRES_USER=aliasuser \
        -e POSTGRES_PASSWORD=aliaspass123 \
        -e POSTGRES_DB=aliasdb \
        -v api_test_alias_selfhosted:/relizaio/postgresql \
        $SELFHOSTED_IMAGE >/dev/null 2>&1
    
    sleep 15
    docker exec api_alias_selfhosted bash -c 'until pg_isready -U aliasuser; do sleep 1; done' >/dev/null 2>&1
    local selfhosted_alias_test=$(docker exec api_alias_selfhosted bash -c 'PGPASSWORD=aliaspass123 psql -U aliasuser -d aliasdb -c "SELECT current_database();" >/dev/null 2>&1' && echo "SUCCESS" || echo "FAILED")
    docker stop api_alias_selfhosted >/dev/null 2>&1 && docker rm api_alias_selfhosted >/dev/null 2>&1
    
    docker volume rm api_test_alias_bitnami api_test_alias_selfhosted >/dev/null 2>&1
    
    if [[ "$bitnami_alias_test" == "SUCCESS" && "$selfhosted_alias_test" == "SUCCESS" ]]; then
        echo "    ✓ POSTGRES_* variable aliasing: Compatible"
    else
        echo "    ✗ POSTGRES_* variable aliasing: Incompatible (Bitnami: $bitnami_alias_test, Self-hosted: $selfhosted_alias_test)"
    fi
    
    # Test 2: Init scripts execution
    echo "  Testing init scripts execution..."
    
    # Create test init script
    mkdir -p /tmp/init-test
    cat > /tmp/init-test/01-test.sql << 'EOF'
CREATE TABLE init_test (id SERIAL PRIMARY KEY, message TEXT);
INSERT INTO init_test (message) VALUES ('Init script executed');
EOF
    
    docker volume create api_test_init_bitnami >/dev/null 2>&1
    docker volume create api_test_init_selfhosted >/dev/null 2>&1
    
    # Test Bitnami init scripts
    docker run -d --name api_init_bitnami \
        -e POSTGRESQL_USERNAME=inituser \
        -e POSTGRESQL_PASSWORD=initpass123 \
        -e POSTGRESQL_DATABASE=initdb \
        -v api_test_init_bitnami:/bitnami/postgresql \
        -v /tmp/init-test:/docker-entrypoint-initdb.d \
        $ORIGINAL_BITNAMI_IMAGE >/dev/null 2>&1
    
    sleep 20
    docker exec api_init_bitnami bash -c 'until pg_isready -U inituser; do sleep 1; done' >/dev/null 2>&1
    local bitnami_init_test=$(docker exec api_init_bitnami bash -c 'PGPASSWORD=initpass123 psql -U inituser -d initdb -c "SELECT message FROM init_test;" 2>/dev/null | grep -q "Init script executed"' && echo "SUCCESS" || echo "FAILED")
    docker stop api_init_bitnami >/dev/null 2>&1 && docker rm api_init_bitnami >/dev/null 2>&1
    
    # Test Self-hosted init scripts
    docker run -d --name api_init_selfhosted \
        -e POSTGRESQL_USERNAME=inituser \
        -e POSTGRESQL_PASSWORD=initpass123 \
        -e POSTGRESQL_DATABASE=initdb \
        -v api_test_init_selfhosted:/relizaio/postgresql \
        -v /tmp/init-test:/docker-entrypoint-initdb.d \
        $SELFHOSTED_IMAGE >/dev/null 2>&1
    
    sleep 20
    docker exec api_init_selfhosted bash -c 'until pg_isready -U inituser; do sleep 1; done' >/dev/null 2>&1
    local selfhosted_init_test=$(docker exec api_init_selfhosted bash -c 'PGPASSWORD=initpass123 psql -U inituser -d initdb -c "SELECT message FROM init_test;" 2>/dev/null | grep -q "Init script executed"' && echo "SUCCESS" || echo "FAILED")
    docker stop api_init_selfhosted >/dev/null 2>&1 && docker rm api_init_selfhosted >/dev/null 2>&1
    
    docker volume rm api_test_init_bitnami api_test_init_selfhosted >/dev/null 2>&1
    rm -rf /tmp/init-test
    
    if [[ "$bitnami_init_test" == "SUCCESS" && "$selfhosted_init_test" == "SUCCESS" ]]; then
        echo "    ✓ Init scripts execution: Compatible"
    else
        echo "    ✗ Init scripts execution: Incompatible (Bitnami: $bitnami_init_test, Self-hosted: $selfhosted_init_test)"
    fi
    
    # Test 3: Configuration variables
    echo "  Testing Bitnami-specific configuration variables..."
    
    docker volume create api_test_config_bitnami >/dev/null 2>&1
    docker volume create api_test_config_selfhosted >/dev/null 2>&1
    
    # Test max_connections configuration
    docker run -d --name api_config_bitnami \
        -e POSTGRESQL_USERNAME=configuser \
        -e POSTGRESQL_PASSWORD=configpass123 \
        -e POSTGRESQL_DATABASE=configdb \
        -e POSTGRESQL_MAX_CONNECTIONS=150 \
        -v api_test_config_bitnami:/bitnami/postgresql \
        $ORIGINAL_BITNAMI_IMAGE >/dev/null 2>&1
    
    sleep 15
    docker exec api_config_bitnami bash -c 'until pg_isready -U configuser; do sleep 1; done' >/dev/null 2>&1
    local bitnami_config_test=$(docker exec api_config_bitnami bash -c 'PGPASSWORD=configpass123 psql -U configuser -d configdb -c "SHOW max_connections;" 2>/dev/null | grep -q "150"' && echo "SUCCESS" || echo "FAILED")
    docker stop api_config_bitnami >/dev/null 2>&1 && docker rm api_config_bitnami >/dev/null 2>&1
    
    docker run -d --name api_config_selfhosted \
        -e POSTGRESQL_USERNAME=configuser \
        -e POSTGRESQL_PASSWORD=configpass123 \
        -e POSTGRESQL_DATABASE=configdb \
        -e POSTGRESQL_MAX_CONNECTIONS=150 \
        -v api_test_config_selfhosted:/relizaio/postgresql \
        $SELFHOSTED_IMAGE >/dev/null 2>&1
    
    sleep 15
    docker exec api_config_selfhosted bash -c 'until pg_isready -U configuser; do sleep 1; done' >/dev/null 2>&1
    local selfhosted_config_test=$(docker exec api_config_selfhosted bash -c 'PGPASSWORD=configpass123 psql -U configuser -d configdb -c "SHOW max_connections;" 2>/dev/null | grep -q "150"' && echo "SUCCESS" || echo "FAILED")
    docker stop api_config_selfhosted >/dev/null 2>&1 && docker rm api_config_selfhosted >/dev/null 2>&1
    
    docker volume rm api_test_config_bitnami api_test_config_selfhosted >/dev/null 2>&1
    
    if [[ "$bitnami_config_test" == "SUCCESS" && "$selfhosted_config_test" == "SUCCESS" ]]; then
        echo "    ✓ Configuration variables: Compatible"
    else
        echo "    ✗ Configuration variables: Incompatible (Bitnami: $bitnami_config_test, Self-hosted: $selfhosted_config_test)"
    fi
    
    # Test 4: Remote connections configuration
    echo "  Testing remote connections configuration..."
    
    docker volume create api_test_remote_bitnami >/dev/null 2>&1
    docker volume create api_test_remote_selfhosted >/dev/null 2>&1
    
    # Test Bitnami remote connections
    docker run -d --name api_remote_bitnami \
        -e POSTGRESQL_USERNAME=remoteuser \
        -e POSTGRESQL_PASSWORD=remotepass123 \
        -e POSTGRESQL_DATABASE=remotedb \
        -e POSTGRESQL_ALLOW_REMOTE_CONNECTIONS=yes \
        -v api_test_remote_bitnami:/bitnami/postgresql \
        -p 15435:5432 \
        $ORIGINAL_BITNAMI_IMAGE >/dev/null 2>&1
    
    sleep 15
    docker exec api_remote_bitnami bash -c 'until pg_isready -U remoteuser; do sleep 1; done' >/dev/null 2>&1
    local bitnami_remote_test=$(docker exec api_remote_bitnami bash -c 'grep -q "host.*all.*all.*0.0.0.0/0.*md5" /opt/bitnami/postgresql/conf/pg_hba.conf' && echo "SUCCESS" || echo "FAILED")
    docker stop api_remote_bitnami >/dev/null 2>&1 && docker rm api_remote_bitnami >/dev/null 2>&1
    
    # Test Self-hosted remote connections
    docker run -d --name api_remote_selfhosted \
        -e POSTGRESQL_USERNAME=remoteuser \
        -e POSTGRESQL_PASSWORD=remotepass123 \
        -e POSTGRESQL_DATABASE=remotedb \
        -e POSTGRESQL_ALLOW_REMOTE_CONNECTIONS=yes \
        -v api_test_remote_selfhosted:/relizaio/postgresql \
        -p 15436:5432 \
        $SELFHOSTED_IMAGE >/dev/null 2>&1
    
    sleep 15
    docker exec api_remote_selfhosted bash -c 'until pg_isready -U remoteuser; do sleep 1; done' >/dev/null 2>&1
    local selfhosted_remote_test=$(docker exec api_remote_selfhosted bash -c 'grep -q "host.*all.*all.*0.0.0.0/0.*md5" /opt/relizaio/postgresql/conf/pg_hba.conf' && echo "SUCCESS" || echo "FAILED")
    docker stop api_remote_selfhosted >/dev/null 2>&1 && docker rm api_remote_selfhosted >/dev/null 2>&1
    
    docker volume rm api_test_remote_bitnami api_test_remote_selfhosted >/dev/null 2>&1
    
    if [[ "$bitnami_remote_test" == "SUCCESS" && "$selfhosted_remote_test" == "SUCCESS" ]]; then
        echo "    ✓ Remote connections configuration: Compatible"
    else
        echo "    ✗ Remote connections configuration: Incompatible (Bitnami: $bitnami_remote_test, Self-hosted: $selfhosted_remote_test)"
    fi
    
    echo "  ✓ Bitnami API compatibility testing complete"
}

main() {
    echo "• Building self-hosted image..."
    if ! docker build -t $SELFHOSTED_IMAGE ./debian-13/ >/dev/null 2>&1; then
        echo "  ✗ Self-hosted image build failed"
        echo "Build logs:"
        docker build -t $SELFHOSTED_IMAGE ./debian-13/
        return 1
    fi
    echo "  ✓ Self-hosted image built"
    echo
    
    # Test original Bitnami API first
    if ! test_original_bitnami_api; then
        echo "  ✗ Original Bitnami API test failed"
        return 1
    fi
    echo
    
    # Run existing migration test
    setup_bitnami || { echo "  ✗ Bitnami setup failed"; return 1; }
    populate_data || { echo "  ✗ Data population failed"; return 1; }
    migrate_to_selfhosted || { echo "  ✗ Migration to self-hosted failed"; return 1; }
    validate_data || { echo "  ✗ Data validation failed"; return 1; }
    test_queries || { echo "  ✗ Query testing failed"; return 1; }
    check_version || { echo "  ✗ Version check failed"; return 1; }
    echo
    
    # Compare APIs
    compare_api_compatibility
    echo
    
    echo "=== Migration & Compatibility Test Completed Successfully ==="
}

[[ "$1" == "run" ]] && main || echo "Usage: $0 run"
