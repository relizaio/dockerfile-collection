#!/usr/bin/env node
//
// Verifies the age filter against live npmjs.org metadata.
//
//   node test-age-filter.js
//   VERDACCIO_URL=http://verdaccio:4873 QUARANTINE_DAYS=10 node test-age-filter.js
//
// Exits non-zero when the filter misbehaves, so it can gate a build. For each
// package it asserts three things, because the filter can fail in three ways:
//   1. no version published inside the quarantine window is served (a leak is
//      the failure this plugin exists to prevent);
//   2. no version older than the window is missing (over-filtering would
//      silently strip a registry down to nothing);
//   3. every dist-tag resolves to a version the mirror actually serves -- an
//      unreconciled tag makes `npm install pkg` fail outright rather than
//      quietly install the older version, which is the whole point.
//
// Packages are chosen to publish often; one that happens to have had no
// release inside the window can only prove (2) and (3), so that case is
// called out rather than counted as strong evidence.

const VERDACCIO_URL = process.env.VERDACCIO_URL || 'http://localhost:4873';
const NPMJS_URL = 'https://registry.npmjs.org';
const QUARANTINE_DAYS = Number(process.env.QUARANTINE_DAYS || 7);
const TEST_PACKAGES = (process.env.TEST_PACKAGES || 'eslint,@types/node,typescript,react,axios')
    .split(',').map(s => s.trim()).filter(Boolean);

const WINDOW_MS = QUARANTINE_DAYS * 24 * 60 * 60 * 1000;

async function fetchMetadata (base, name) {
    const res = await fetch(`${base}/${name.replace('/', '%2f')}`);
    if (!res.ok) throw new Error(`GET ${base}/${name} -> HTTP ${res.status}`);
    return res.json();
}

const ageMs = (time, version) =>
    time && time[version] ? Date.now() - Date.parse(time[version]) : NaN;
const days = ms => (ms / 86400000).toFixed(1);

async function testPackage (name) {
    const [upstream, mirror] = await Promise.all([
        fetchMetadata(NPMJS_URL, name),
        fetchMetadata(VERDACCIO_URL, name),
    ]);

    const upstreamVersions = Object.keys(upstream.versions || {});
    const mirrored = new Set(Object.keys(mirror.versions || {}));
    const inWindow = upstreamVersions.filter(v => ageMs(upstream.time, v) < WINDOW_MS);
    const aged = upstreamVersions.filter(v => ageMs(upstream.time, v) >= WINDOW_MS);

    const leaked = inWindow.filter(v => mirrored.has(v));
    const dropped = aged.filter(v => !mirrored.has(v));

    const tags = mirror['dist-tags'] || {};
    const unresolvable = Object.entries(tags).filter(([, v]) => !mirrored.has(v));
    const staleTagged = Object.entries(tags).filter(([, v]) => ageMs(upstream.time, v) < WINDOW_MS);

    const ok = !leaked.length && !dropped.length && !unresolvable.length && !staleTagged.length;

    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);
    console.log(`      upstream ${upstreamVersions.length} versions, ${inWindow.length} inside the ${QUARANTINE_DAYS}d window; mirror serves ${mirrored.size}`);
    if (inWindow.length) {
        const newest = inWindow.sort((a, b) => ageMs(upstream.time, a) - ageMs(upstream.time, b))[0];
        console.log(`      newest upstream ${newest} (${days(ageMs(upstream.time, newest))}d old) -> ${mirrored.has(newest) ? 'LEAKED' : 'withheld'}`);
    } else {
        console.log('      no upstream release inside the window -- this package cannot prove filtering');
    }
    console.log(`      dist-tags.latest: upstream ${upstream['dist-tags']?.latest} / mirror ${tags.latest}`);
    if (leaked.length) console.log(`      LEAKED: ${leaked.join(', ')}`);
    if (dropped.length) console.log(`      OVER-FILTERED: ${dropped.slice(0, 5).join(', ')}${dropped.length > 5 ? ` +${dropped.length - 5} more` : ''}`);
    if (unresolvable.length) console.log(`      UNRESOLVABLE TAGS: ${unresolvable.map(([t, v]) => `${t}=${v}`).join(', ')}`);
    if (staleTagged.length) console.log(`      TAGS POINT INSIDE WINDOW: ${staleTagged.map(([t, v]) => `${t}=${v}`).join(', ')}`);

    return ok;
}

async function main () {
    console.log(`Age filter check -- mirror ${VERDACCIO_URL}, quarantine ${QUARANTINE_DAYS} days`);

    const ping = await fetch(`${VERDACCIO_URL}/-/ping`).catch(e => {
        throw new Error(`cannot reach ${VERDACCIO_URL}: ${e.message}`);
    });
    if (!ping.ok) throw new Error(`${VERDACCIO_URL}/-/ping -> HTTP ${ping.status}`);

    let failed = 0;
    for (const name of TEST_PACKAGES) {
        try {
            if (!await testPackage(name)) failed++;
        } catch (e) {
            console.log(`ERROR ${name}: ${e.message}`);
            failed++;
        }
    }
    console.log(`\n${failed ? `${failed} of ${TEST_PACKAGES.length} FAILED` : `all ${TEST_PACKAGES.length} packages pass`}`);
    process.exit(failed ? 1 : 0);
}

main().catch(e => { console.error(`ERROR: ${e.message}`); process.exit(1); });
