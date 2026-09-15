/**
 * Tests for the minimal tag (tag.js): conversion event capture.
 *
 * Uses the shared browser sandbox to evaluate tag.js, calls the public
 * TRACE.conversion() API, and inspects the payloads handed to sendBeacon.
 *
 * Run: npm test  (from client/) or: node --test test/conversion.test.mjs
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadTag } from './helpers/tag-sandbox.mjs';

/** Conversion event payloads, in emission order */
function conversionEvents(sent) {
  return sent.filter((e) => e.payload.type === 'conversion').map((e) => e.payload);
}

test('conversion with type and revenue', () => {
  const tag = loadTag();

  tag.window.TRACE.conversion({ conversion_type: 'purchase', revenue: 49.99 });

  const events = conversionEvents(tag.sent);
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'conversion');
  assert.equal(events[0].conversion_type, 'purchase');
  assert.equal(events[0].revenue, 49.99);
});

test('string shorthand sets conversion_type', () => {
  const tag = loadTag();

  tag.window.TRACE.conversion('signup');

  const event = conversionEvents(tag.sent)[0];
  assert.equal(event.type, 'conversion');
  assert.equal(event.conversion_type, 'signup');
  assert.equal(event.revenue, undefined);
});

test('options.type becomes conversion_type, payload type stays conversion', () => {
  const tag = loadTag();

  // A caller passing type: 'lead' means "a lead conversion" — the event
  // type itself must stay 'conversion' or the attribution queries
  // (type = 'conversion') will not count it.
  tag.window.TRACE.conversion({ type: 'lead', revenue: 10 });

  const event = conversionEvents(tag.sent)[0];
  assert.equal(event.type, 'conversion');
  assert.equal(event.conversion_type, 'lead');
  assert.equal(event.revenue, 10);
});

test('no options defaults to a generic conversion', () => {
  const tag = loadTag();

  tag.window.TRACE.conversion();

  const event = conversionEvents(tag.sent)[0];
  assert.equal(event.type, 'conversion');
  assert.equal(event.conversion_type, 'conversion');
});

test('extra keys pass through as params (currency, order_id)', () => {
  const tag = loadTag();

  tag.window.TRACE.conversion({
    conversion_type: 'purchase',
    revenue: 25,
    currency: 'USD',
    order_id: 'A-1001',
  });

  const event = conversionEvents(tag.sent)[0];
  assert.equal(event.currency, 'USD');
  assert.equal(event.order_id, 'A-1001');
});

test('conversion payload carries the standard envelope fields', () => {
  const tag = loadTag();

  tag.window.TRACE.conversion({ conversion_type: 'purchase', revenue: 1 });

  const event = conversionEvents(tag.sent)[0];
  assert.equal(event.url, 'https://site.example/page?utm_source=test');
  assert.ok(event.sid, 'session id should be present');
  assert.ok(event.uid, 'user id should be present');
  assert.ok(event.pv, 'page view id should be present');
  assert.ok(!Number.isNaN(Date.parse(event.ts)), 'ts should be ISO 8601');
  assert.match(event.ts, /Z$/);
});

test('ordinary tracking is unaffected by a conversion call', async () => {
  const tag = loadTag();

  tag.window.TRACE.conversion('purchase');

  // The load pageview is still there, and the conversion was sent through
  // the same beacon channel
  assert.equal(tag.sent[0].payload.type, 'pageview');
  assert.equal(conversionEvents(tag.sent).length, 1);
});
