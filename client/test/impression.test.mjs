/**
 * Tests for the minimal tag (tag.js): impression event capture.
 *
 * Uses the shared browser sandbox to evaluate tag.js, calls the public
 * TRACE.impression() API, and inspects the payloads handed to sendBeacon.
 *
 * Run: npm test  (from client/) or: node --test test/impression.test.mjs
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadTag } from './helpers/tag-sandbox.mjs';

/** Impression event payloads, in emission order */
function impressionEvents(sent) {
  return sent.filter((e) => e.payload.type === 'impression').map((e) => e.payload);
}

test('impression with creative_id sends type=impression and a generated imp_id', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({ creative_id: 'creative-7', ad_slot: 'hero' });

  const events = impressionEvents(tag.sent);
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'impression');
  assert.equal(events[0].creative_id, 'creative-7');
  assert.equal(events[0].ad_slot, 'hero');
  assert.ok(events[0].imp_id, 'imp_id should be generated when not supplied');
});

test('string shorthand sets creative_id', () => {
  const tag = loadTag();

  tag.window.TRACE.impression('creative-9');

  const event = impressionEvents(tag.sent)[0];
  assert.equal(event.type, 'impression');
  assert.equal(event.creative_id, 'creative-9');
  assert.ok(event.imp_id);
});

test('repeat calls for the same creative are dropped (client dedup)', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({ creative_id: 'creative-7' });
  tag.window.TRACE.impression({ creative_id: 'creative-7', in_view_ms: 1000 });
  tag.window.TRACE.impression('creative-7');

  const events = impressionEvents(tag.sent);
  assert.equal(events.length, 1, 'one impression per creative per page view');
});

test('distinct creatives each send their own impression with distinct imp_ids', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({ creative_id: 'creative-a' });
  tag.window.TRACE.impression({ creative_id: 'creative-b' });

  const events = impressionEvents(tag.sent);
  assert.equal(events.length, 2);
  assert.notEqual(events[0].imp_id, events[1].imp_id);
});

test('same creative in different ad slots counts twice', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({ creative_id: 'creative-7', ad_slot: 'hero' });
  tag.window.TRACE.impression({ creative_id: 'creative-7', ad_slot: 'sidebar' });

  const events = impressionEvents(tag.sent);
  assert.equal(events.length, 2, 'slot distinguishes two placements of one creative');
  assert.notEqual(events[0].imp_id, events[1].imp_id);
});

test('caller-supplied imp_id is used verbatim and dedups repeats', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({ imp_id: 'adserver-imp-42', creative_id: 'creative-1' });
  tag.window.TRACE.impression({ imp_id: 'adserver-imp-42', creative_id: 'creative-1' });

  const events = impressionEvents(tag.sent);
  assert.equal(events.length, 1);
  assert.equal(events[0].imp_id, 'adserver-imp-42');
});

test('generated imp_id is scoped to the page view', () => {
  const tag = loadTag();

  // The load pageview carries the page-view ID in its envelope (pv)
  const pv = tag.sent.find((e) => e.payload.type === 'pageview').payload.pv;
  assert.ok(pv, 'pageview envelope carries pv');

  tag.window.TRACE.impression({ creative_id: 'creative-7' });

  const event = impressionEvents(tag.sent)[0];
  assert.equal(
    event.imp_id,
    pv + ':creative-7',
    'generated imp_id must embed the page-view ID so the flusher dedup does not collide across page views'
  );
});

test('a stray options.type is dropped so the payload type stays impression', () => {
  const tag = loadTag();

  // A caller passing type: 'view' must not reclassify the event — the
  // funnel/impression queries count type = 'impression'.
  tag.window.TRACE.impression({ type: 'view', creative_id: 'creative-2' });

  const event = impressionEvents(tag.sent)[0];
  assert.equal(event.type, 'impression');
  assert.equal(event.creative, undefined);
});

test('viewability and attribution params pass through', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({
    creative_id: 'creative-3',
    in_view_ms: 2400,
    utm_source: 'taboola',
    utm_campaign: 'camp-1',
  });

  const event = impressionEvents(tag.sent)[0];
  assert.equal(event.in_view_ms, 2400);
  assert.equal(event.utm_source, 'taboola');
  assert.equal(event.utm_campaign, 'camp-1');
});

test('impression payload carries the standard envelope fields', () => {
  const tag = loadTag();

  tag.window.TRACE.impression({ creative_id: 'creative-7' });

  const event = impressionEvents(tag.sent)[0];
  assert.equal(event.url, 'https://site.example/page?utm_source=test');
  assert.ok(event.sid, 'session id should be present');
  assert.ok(event.uid, 'user id should be present');
  assert.ok(event.pv, 'page view id should be present');
  assert.ok(!Number.isNaN(Date.parse(event.ts)), 'ts should be ISO 8601');
  assert.match(event.ts, /Z$/);
});

test('ordinary tracking is unaffected by an impression call', () => {
  const tag = loadTag();

  tag.window.TRACE.impression('creative-7');

  // The load pageview is still there, and the impression was sent through
  // the same beacon channel
  assert.equal(tag.sent[0].payload.type, 'pageview');
  assert.equal(impressionEvents(tag.sent).length, 1);
});
