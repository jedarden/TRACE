/**
 * Tests for the minimal tag (tag.js): scroll depth event emission.
 *
 * tag.js is a browser IIFE, so each test builds a DOM/Window sandbox,
 * evaluates the tag source inside it, drives scroll positions through
 * the mocked window, and inspects the payloads handed to sendBeacon.
 *
 * Run: npm test  (from client/) or: node --test test/tag.test.mjs
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadTag, sleep } from './helpers/tag-sandbox.mjs';

/** Scroll event payloads, in emission order */
function scrollEvents(sent) {
  return sent.filter((e) => e.payload.type === 'scroll').map((e) => e.payload);
}

test('pageview fires on load, no scroll events until scrolling', async () => {
  const tag = loadTag();

  assert.equal(tag.sent.length, 1);
  assert.equal(tag.sent[0].payload.type, 'pageview');

  // The scroll listener exists but a no-op scroll (no position change
  // past a threshold) must not emit anything
  tag.scrollTo(0);
  await sleep(150);

  assert.equal(scrollEvents(tag.sent).length, 0);
});

test('thresholds fire once each, in order, with max_scroll_depth', async () => {
  const tag = loadTag(); // scrollable = 4000px

  tag.scrollTo(2000); // 50% — jumps past 25% too
  await sleep(150);
  tag.scrollTo(1000); // back to 25% territory — already sent, no new events
  await sleep(150);
  tag.scrollTo(3100); // ~78% — crosses 75%
  await sleep(150);
  tag.scrollTo(4000); // 100%
  await sleep(150);
  tag.scrollTo(4000); // still 100% — no duplicates
  await sleep(150);

  const depths = scrollEvents(tag.sent).map((e) => e.scroll_depth);
  assert.deepEqual(depths, [25, 50, 75, 100]);

  const maxes = scrollEvents(tag.sent).map((e) => e.max_scroll_depth);
  assert.deepEqual(maxes, [50, 50, 78, 100]);
  for (let i = 1; i < maxes.length; i++) {
    assert.ok(maxes[i] >= maxes[i - 1], 'max_scroll_depth must be monotonic');
  }
});

test('short page (fits viewport) counts as fully viewed', async () => {
  const tag = loadTag({ scrollHeight: 800, innerHeight: 1000 });

  tag.scrollTo(0);
  await sleep(150);

  const depths = scrollEvents(tag.sent).map((e) => e.scroll_depth);
  assert.deepEqual(depths, [25, 50, 75, 100]);
  assert.deepEqual(
    scrollEvents(tag.sent).map((e) => e.max_scroll_depth),
    [100, 100, 100, 100],
  );
});

test('scroll payload carries the standard envelope fields', async () => {
  const tag = loadTag();

  tag.scrollTo(2000);
  await sleep(150);

  const event = scrollEvents(tag.sent)[0];
  assert.ok(event, 'expected a scroll event');
  assert.equal(event.url, 'https://site.example/page?utm_source=test');
  assert.ok(event.sid, 'session id should be present');
  assert.ok(event.uid, 'user id should be present');
  assert.ok(event.pv, 'page view id should be present');
  assert.ok(!Number.isNaN(Date.parse(event.ts)), 'ts should be ISO 8601');
  assert.match(event.ts, /Z$/);
});
