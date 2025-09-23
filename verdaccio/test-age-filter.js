#!/usr/bin/env node

const https = require('https');
const http = require('http');

// Test configuration
const VERDACCIO_URL = 'http://localhost:4873';
const NPMJS_URL = 'https://registry.npmjs.org';

// Test packages - choose packages that likely have recent versions
const TEST_PACKAGES = [
    'axios'
];

async function fetchPackageMetadata(url, packageName) {
    return new Promise((resolve, reject) => {
        const client = url.startsWith('https') ? https : http;
        
        client.get(`${url}/${packageName}`, (res) => {
            let data = '';
            
            res.on('data', (chunk) => {
                data += chunk;
            });
            
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data));
                } catch (e) {
                    reject(e);
                }
            });
        }).on('error', reject);
    });
}

function analyzeVersions(metadata, source) {
    if (!metadata.versions) {
        return { total: 0, recent: 0, old: 0 };
    }
    
    const quarantineDays = 10 * 24 * 60 * 60 * 1000;
    const now = Date.now();
    
    let total = 0;
    let recent = 0; // Less than 7 days old
    let old = 0;    // 7+ days old
    
    for (const [version, data] of Object.entries(metadata.versions)) {
        total++;
        const publishedStr = metadata.time?.[version];
        const publishedAt = publishedStr ? Date.parse(publishedStr) : NaN;
        const age = Number.isFinite(publishedAt) ? (now - publishedAt) : 0;
        
        // console.log(`Version ${version} is ${age}ms old`);
        if (age < quarantineDays) {
            recent++;
        } else {
            old++;
        }
    }
    
    console.log(`\n${source} - ${metadata.name}:`);
    console.log(`  Total versions: ${total}`);
    console.log(`  Recent versions (< 7 days): ${recent}`);
    console.log(`  Old versions (>= 7 days): ${old}`);
    
    return { total, recent, old };
}

async function testPackage(packageName) {
    console.log(`\n${'='.repeat(50)}`);
    console.log(`Testing package: ${packageName}`);
    console.log(`${'='.repeat(50)}`);
    
    try {
        // Fetch from npmjs.org (original)
        console.log('Fetching from npmjs.org...');
        const npmjsData = await fetchPackageMetadata(NPMJS_URL, packageName);
        const npmjsStats = analyzeVersions(npmjsData, 'NPMJS.ORG');
        
        // Fetch from Verdaccio (filtered)
        console.log('\nFetching from Verdaccio...');
        const verdaccioData = await fetchPackageMetadata(VERDACCIO_URL, packageName);
        const verdaccioStats = analyzeVersions(verdaccioData, 'VERDACCIO (FILTERED)');
        
        // Analysis
        console.log(`\n📊 ANALYSIS:`);
        console.log(`  Original total versions: ${npmjsStats.total}`);
        console.log(`  Filtered total versions: ${verdaccioStats.total}`);
        console.log(`  Versions filtered out: ${npmjsStats.total - verdaccioStats.total}`);
        console.log(`  Recent versions in original: ${npmjsStats.recent}`);
        console.log(`  Recent versions in filtered: ${verdaccioStats.recent}`);
        
        // Verification
        if (verdaccioStats.recent === 0 && npmjsStats.recent > 0) {
            console.log(`✅ SUCCESS: Age filter is working! ${npmjsStats.recent} recent versions were filtered out.`);
        } else if (npmjsStats.recent === 0) {
            console.log(`ℹ️  INFO: No recent versions found in this package to filter.`);
        } else {
            console.log(`❌ WARNING: Age filter may not be working. Recent versions still present.`);
        }
        
    } catch (error) {
        console.error(`❌ Error testing ${packageName}:`, error.message);
    }
}

async function runTests() {
    console.log('🧪 Testing Verdaccio Age Filter Plugin');
    console.log('=====================================');
    console.log(`Verdaccio URL: ${VERDACCIO_URL}`);
    console.log(`Test packages: ${TEST_PACKAGES.join(', ')}`);
    
    // Test Verdaccio connectivity first
    try {
        console.log('\n🔍 Testing Verdaccio connectivity...');
        // Test with a simple ping endpoint instead of package metadata
        const pingResponse = await new Promise((resolve, reject) => {
            const client = VERDACCIO_URL.startsWith('https') ? https : http;
            client.get(`${VERDACCIO_URL}/-/ping`, (res) => {
                let data = '';
                res.on('data', (chunk) => data += chunk);
                res.on('end', () => resolve({ statusCode: res.statusCode, data }));
            }).on('error', reject);
        });
        
        if (pingResponse.statusCode === 200) {
            console.log('✅ Verdaccio is accessible');
        } else {
            throw new Error(`Unexpected status code: ${pingResponse.statusCode}`);
        }
    } catch (error) {
        console.error('❌ Cannot connect to Verdaccio:', error.message);
        console.log('Make sure Verdaccio is running on http://localhost:4873');
        return;
    }
    
    // Run tests for each package
    for (const packageName of TEST_PACKAGES) {
        await testPackage(packageName);
        
        // Add delay between requests to be nice to the registries
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    console.log('\n🏁 Testing completed!');
    console.log('\nTo manually verify:');
    console.log('1. Visit http://localhost:4873 in your browser');
    console.log('2. Search for a popular package like "react"');
    console.log('3. Check if recent versions (< 7 days old) are missing');
}

// Run the tests
runTests().catch(console.error);
