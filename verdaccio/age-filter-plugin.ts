// Note: avoid ESM default export; Verdaccio expects CommonJS (module.exports)
// Type annotations are kept generic to avoid tight coupling with @verdaccio/types at runtime.

class AgeFilterPlugin {
  private quarantineDays: number;

  constructor(config: any, options: any) {
    console.log('PSDEBUG: constructed plugin')
    this.quarantineDays = (config && Number(config.quarantineDays)) || 7;
  }

  // Called when fetching package metadata
  filter_metadata(metadata: any) {
    console.log(`PSDEBUG: filtering metadata`);
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
        console.log(`[age-filter] filtered ${removed} version(s) from ${metadata.name || 'unknown'}`);

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
      console.error('[age-filter] error in filter_metadata:', e);
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
      // Intercept package metadata requests
      app.use((req: any, res: any, next: any) => {
        // Only intercept GET requests for package metadata (not tarballs, etc.)
        if (req.method !== 'GET' || !req.url || req.url.includes('/-/') || req.url.includes('.tgz')) {
          return next();
        }

        console.log(`PSDEBUG: intercepting request for ${req.url}`);

        // Intercept both res.json and res.send for different response patterns
        const originalJson = res.json?.bind(res);
        const originalSend = res.send?.bind(res);
        const originalEnd = res.end?.bind(res);

        // Override res.json
        if (originalJson) {
          res.json = (body: any) => {
            console.log(`PSDEBUG: res.json called with body type: ${typeof body}`);
            try {
              if (body && typeof body === 'object' && body.versions && body.time) {
                console.log(`PSDEBUG: filtering JSON body for ${body.name}`);
                body = plugin.filter_metadata(body);
              }
            } catch (e) {
              console.error('[age-filter] middleware json error:', e);
            }
            return originalJson(body);
          };
        }

        // Override res.send
        if (originalSend) {
          res.send = (body: any) => {
            console.log(`PSDEBUG: res.send called with body type: ${typeof body}`);
            try {
              if (typeof body === 'string') {
                const parsed = JSON.parse(body);
                if (parsed && parsed.versions && parsed.time) {
                  console.log(`PSDEBUG: filtering string body for ${parsed.name}`);
                  const filtered = plugin.filter_metadata(parsed);
                  body = JSON.stringify(filtered);
                }
              } else if (body && typeof body === 'object' && body.versions && body.time) {
                console.log(`PSDEBUG: filtering object body for ${body.name}`);
                body = plugin.filter_metadata(body);
              }
            } catch (e) {
              console.error('[age-filter] middleware send error:', e);
            }
            return originalSend(body);
          };
        }

        // Override res.end
        if (originalEnd) {
          res.end = (chunk: any, encoding?: any) => {
            console.log(`PSDEBUG: res.end called with chunk type: ${typeof chunk}`);
            try {
              if (typeof chunk === 'string') {
                const parsed = JSON.parse(chunk);
                if (parsed && parsed.versions && parsed.time) {
                  console.log(`PSDEBUG: filtering end chunk for ${parsed.name}`);
                  const filtered = plugin.filter_metadata(parsed);
                  chunk = JSON.stringify(filtered);
                }
              }
            } catch (e) {
              console.error('[age-filter] middleware end error:', e);
            }
            return originalEnd.call(res, chunk, encoding);
          };
        }

        return next();
      });
    }
  };
};
