FROM verdaccio/verdaccio:6.1.6@sha256:44af8dec4b8bfb9b940263f56ee5f371484515e4397eea56ab9c942500ab9dfa

# Switch to root to install dependencies
USER root

# Create plugin directory
RUN mkdir -p /verdaccio/plugins/age-filter

# Copy plugin files
COPY package.json /verdaccio/plugins/age-filter/
COPY package-lock.json /verdaccio/plugins/age-filter/
COPY age-filter-plugin.ts /verdaccio/plugins/age-filter/
COPY tsconfig.json /verdaccio/plugins/age-filter/

# Copy Verdaccio configuration
COPY config.yaml /verdaccio/conf/config.yaml
RUN chown verdaccio:nogroup -R /verdaccio/plugins/age-filter

# Switch back to verdaccio user BEFORE npm operations
USER verdaccio

# Install plugin dependencies and build (now as non-root user)
WORKDIR /verdaccio/plugins/age-filter
RUN npm ci
RUN npx tsc

# Expose port
EXPOSE 4873

# Start Verdaccio
CMD ["verdaccio", "--config", "/verdaccio/conf/config.yaml"]
