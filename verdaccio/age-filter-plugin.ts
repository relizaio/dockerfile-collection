import { IPluginStorageFilter, PluginOptions } from '@verdaccio/types';

export default class AgeFilterPlugin implements IPluginStorageFilter<any> {
  constructor(config: any, options: PluginOptions<any>) {
    console.log('Age Filter Plugin initialized');
  }

  // Called when fetching package metadata
  filter_metadata(metadata: any) {
    const sevenDays = 7 * 24 * 60 * 60 * 1000;
    const now = Date.now();

    if (metadata.versions) {
      const originalVersionCount = Object.keys(metadata.versions).length;
      
      for (const [ver, data] of Object.entries(metadata.versions)) {
        const versionData = data as any;
        const created = new Date(versionData.time?.created ?? now);
        
        if (now - created.getTime() < sevenDays) {
          delete metadata.versions[ver];
        }
      }
      
      const filteredVersionCount = Object.keys(metadata.versions).length;
      
      if (originalVersionCount !== filteredVersionCount) {
        console.log(`Age Filter: Filtered ${originalVersionCount - filteredVersionCount} versions from ${metadata.name || 'unknown package'}`);
      }
    }
    
    return metadata;
  }
}
