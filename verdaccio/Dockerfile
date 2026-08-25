# The plugin is TypeScript, but nothing it needs at runtime is: it imports
# no modules, so the compiled .js plus a package.json (which is how Verdaccio
# discovers the filter) is the whole runtime footprint. Compiling in a
# discarded stage keeps typescript/@types/node -- build-time only -- out of
# the shipped image, so they cannot show up in its SBOM or its CVE count.
FROM verdaccio/verdaccio:6.10.0@sha256:7dacad0ad776219bd0fd88ebbc2156fa7849041aed9bc99796e1e7cf7a29b141 AS plugin-build

USER root
WORKDIR /build
COPY package.json package-lock.json tsconfig.json age-filter-plugin.ts ./
RUN npm ci --include=dev && npx tsc -p tsconfig.json

FROM verdaccio/verdaccio:6.10.0@sha256:7dacad0ad776219bd0fd88ebbc2156fa7849041aed9bc99796e1e7cf7a29b141

# Verdaccio discovers a filter by package name under the plugins directory.
USER root
RUN mkdir -p /verdaccio/plugins/verdaccio-age-filter

COPY --from=plugin-build /build/age-filter-plugin.js /verdaccio/plugins/verdaccio-age-filter/
COPY package.json /verdaccio/plugins/verdaccio-age-filter/

# Verdaccio configuration
COPY config.yaml /verdaccio/conf/config.yaml
RUN chown verdaccio -R /verdaccio

USER verdaccio

# Expose port
EXPOSE 4873

# Start Verdaccio
CMD ["verdaccio", "--config", "/verdaccio/conf/config.yaml"]
