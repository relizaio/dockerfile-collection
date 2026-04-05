FROM verdaccio/verdaccio:6.3.2@sha256:3f533981ed514088088df91cf57fcb422c5a5193e657dd609b6bca425b81c13c

# Switch to root to install dependencies
USER root

# Create plugin directory under node_modules so Verdaccio can discover it by package name
RUN mkdir -p /verdaccio/plugins/verdaccio-age-filter

# Copy plugin files
COPY package.json /verdaccio/plugins/verdaccio-age-filter/
COPY package-lock.json /verdaccio/plugins/verdaccio-age-filter/
COPY age-filter-plugin.ts /verdaccio/plugins/verdaccio-age-filter/
COPY tsconfig.json /verdaccio/plugins/verdaccio-age-filter/

# Copy Verdaccio configuration
COPY config.yaml /verdaccio/conf/config.yaml
RUN chown verdaccio -R /verdaccio

# Switch back to verdaccio user BEFORE npm operations
USER verdaccio

# Install plugin dependencies and build (now as non-root user)
WORKDIR /verdaccio/plugins/verdaccio-age-filter
RUN npm ci --include=dev
RUN npx tsc -p /verdaccio/plugins/verdaccio-age-filter/tsconfig.json

# Expose port
EXPOSE 4873

# Start Verdaccio
CMD ["verdaccio", "--config", "/verdaccio/conf/config.yaml"]
