# Verdaccio with Age Filter Plugin

This project creates a custom Verdaccio Docker image that includes an age filter plugin. The plugin ensures that only npm packages that are at least 7 days old are mirrored from the upstream registry.

## Features

- **Age Filtering**: Only mirrors packages that are 7+ days old
- **Read-Only Proxy**: Allows downloads but disables publishing
- **NPM Registry Mirror**: Proxies all requests to npmjs.org
- **Custom Plugin**: Built-in TypeScript plugin for age filtering

## Files

- `Dockerfile` - Builds the custom Verdaccio image (used by docker-compose)
- `docker-compose.yml` - Compose file to build and run the service
- `config.yaml` - Verdaccio configuration with proxy and plugin settings
- `age-filter-plugin.ts` - TypeScript plugin source code
- `package.json` - Plugin and build dependencies
- `tsconfig.json` - TypeScript compilation configuration
- `test-age-filter.js` - Script to validate the filter behavior against npmjs.org

## Usage

### Start with docker-compose

```bash
# Build and start (rebuilds the image on changes)
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop and remove containers and volumes
docker-compose down -v
```

### Access Verdaccio

Open your browser and navigate to `http://localhost:4873`

### Configure npm to use the proxy

```bash
npm config set registry http://localhost:4873
```

## How the Age Filter Works

The age filter plugin intercepts package metadata requests and:

1. Checks the creation time of each package version
2. Removes versions that are less than 7 (configurable) days old
3. Logs the filtering activity for monitoring
4. Returns the filtered metadata to the client

This ensures that your registry only serves stable, tested packages while still providing access to the latest versions after a reasonable delay.

## Configuration

Quarantine period (in days) can be configured via either environment variable or `config.yaml`.

Precedence (highest to lowest):

1. Environment variable: `VERDACCIO_AGE_FILTER_QUARANTINE_DAYS`
2. `config.yaml` under `filters.age-filter.quarantineDays`
3. Default: `7`

In this repo, `docker-compose.yml` sets:

```yaml
environment:
  - VERDACCIO_AGE_FILTER_QUARANTINE_DAYS=10
```

And `config.yaml` includes:

```yaml
filters:
  age-filter:
    enabled: true
    quarantineDays: 7
```

The plugin code in `age-filter-plugin.ts` reads the environment variable first, then the config value, and falls back to `7`.

## Logs

The plugin logs filtering activity to help you monitor its operation:

```
[age-filter] filtered 3 version(s) from package-name
```

## Security Notes

- The registry is configured as read-only (no publishing allowed)
- All package requests are proxied through npmjs.org
- Storage is persistent through Docker volumes

## Test Steps

```bash
# Stop current setup
docker-compose down -v

# Start new setup
docker-compose up -d --build

# Wait for both services to start
sleep 10

# Test the age filter
node test-age-filter.js