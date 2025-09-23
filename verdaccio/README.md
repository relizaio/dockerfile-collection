# Verdaccio with Age Filter Plugin

This project creates a custom Verdaccio Docker image that includes an age filter plugin. The plugin ensures that only npm packages that are at least 7 days old are mirrored from the upstream registry.

## Features

- **Age Filtering**: Only mirrors packages that are 7+ days old
- **Read-Only Proxy**: Allows downloads but disables publishing
- **NPM Registry Mirror**: Proxies all requests to npmjs.org
- **Custom Plugin**: Built-in TypeScript plugin for age filtering

## Files

- `Dockerfile` - Builds the custom Verdaccio image
- `config.yaml` - Verdaccio configuration with proxy and plugin settings
- `age-filter-plugin.ts` - TypeScript plugin source code
- `package.json` - Plugin dependencies
- `tsconfig.json` - TypeScript compilation configuration

## Usage

### Build the Docker Image

```bash
docker build -t verdaccio-age-filter .
```

### Run the Container

```bash
docker run -d \
  --name verdaccio-age-filter \
  -p 4873:4873 \
  -v verdaccio-storage:/verdaccio/storage \
  verdaccio-age-filter
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
2. Removes versions that are less than 7 days old
3. Logs the filtering activity for monitoring
4. Returns the filtered metadata to the client

This ensures that your registry only serves stable, tested packages while still providing access to the latest versions after a reasonable delay.

## Configuration

You can modify the age threshold by editing the `sevenDays` constant in `age-filter-plugin.ts`:

```typescript
const sevenDays = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds
```

## Logs

The plugin logs filtering activity to help you monitor its operation:

```
Age Filter: Filtered 3 versions from package-name
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