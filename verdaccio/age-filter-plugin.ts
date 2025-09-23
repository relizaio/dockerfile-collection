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

// Verdaccio filters plugin factory (CommonJS export)
// Loader calls exported function with (config, stuff) and expects an object with filter_metadata
module.exports = function createAgeFilter(config: any, stuff: any) {
  console.log('PSDEBUG: creating filters plugin');
  const plugin = new AgeFilterPlugin(config, stuff);
  return {
    // Verdaccio calls this method when fetching metadata from upstream
    filter_metadata(metadata: any) {
      console.log(`PSDEBUG: filters plugin filter_metadata called for ${metadata?.name || 'unknown'}`);
      return plugin.filter_metadata(metadata);
    }
  };
};
