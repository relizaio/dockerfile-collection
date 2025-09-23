// Note: avoid ESM default export; Verdaccio expects CommonJS (module.exports)
// Type annotations are kept generic to avoid tight coupling with @verdaccio/types at runtime.

class AgeFilterPlugin {
  private quarantineDays: number;
  private logger: { info: (...args: any[]) => void; error: (...args: any[]) => void };

  constructor(config: any, options: any) {
    this.quarantineDays = (config && Number(config.quarantineDays)) || 7;
    // Prefer Verdaccio logger if provided, otherwise fallback to console
    this.logger = (options && options.logger) ? options.logger : console;
    this.logger.info(`[age-filter] init, quarantineDays=${this.quarantineDays}`);
  }

  // Called when fetching package metadata
  filter_metadata(metadata: any) {
    try {
      if (!metadata || !metadata.versions || typeof metadata.versions !== 'object') {
        return metadata;
      }

      const timeMap = (metadata && metadata.time) || {};
      const sevenDays = this.quarantineDays * 24 * 60 * 60 * 1000;
      const now = Date.now();

      const originalVersions = Object.keys(metadata.versions);

      for (const ver of originalVersions) {
        const ts = timeMap[ver];
        const publishedAt = ts ? Date.parse(ts) : NaN;
        if (Number.isFinite(publishedAt) && (now - publishedAt) < sevenDays) {
          delete metadata.versions[ver];
        }
      }

      const remaining = Object.keys(metadata.versions);
      if (remaining.length !== originalVersions.length) {
        const removed = originalVersions.length - remaining.length;
        this.logger.info(`[age-filter] filtered ${removed} version(s) from ${metadata.name || 'unknown'}`);

        // Reconcile dist-tags to point to an existing version
        if (metadata['dist-tags']) {
          for (const tag of Object.keys(metadata['dist-tags'])) {
            const target = metadata['dist-tags'][tag];
            if (!metadata.versions[target]) {
              const newestRemaining = remaining
                .map(v => [v, Date.parse(timeMap[v] || 0)] as [string, number])
                .filter(([, t]) => Number.isFinite(t))
                .sort((a, b) => b[1] - a[1])[0];
              if (newestRemaining) {
                metadata['dist-tags'][tag] = newestRemaining[0];
              } else if (remaining.length) {
                metadata['dist-tags'][tag] = remaining[0];
              } else {
                delete metadata['dist-tags'][tag];
              }
            }
          }
        }
      }

      return metadata;
    } catch (e) {
      this.logger.error('[age-filter] error in filter_metadata:', e);
      return metadata;
    }
  }
}

// Verdaccio middleware factory (CommonJS export)
// Loader calls exported function with (config, stuff) and expects an object with register_middlewares
module.exports = function createAgeFilterMiddleware(config: any, stuff: any) {
  const plugin = new AgeFilterPlugin(config, stuff);
  return {
    // Verdaccio will call this at startup, passing Express app and other services
    register_middlewares(app: any /* Express.Application */, auth: any, storage: any) {
      // Intercept JSON responses for package metadata and apply the age filter.
      app.use((req: any, res: any, next: any) => {
        // Only care about GET requests; skip others
        if (req && req.method !== 'GET') return next();

        // Wrap res.json to post-process metadata payloads
        const originalJson = res.json?.bind(res);
        if (!originalJson) return next();

        res.json = (body: any) => {
          try {
            if (body && typeof body === 'object' && body.versions && body.time) {
              body = plugin.filter_metadata(body);
            }
          } catch (e) {
            plugin['logger'].error('[age-filter] middleware wrap error:', e);
          }
          return originalJson(body);
        };

        return next();
      });
    }
  };
};
