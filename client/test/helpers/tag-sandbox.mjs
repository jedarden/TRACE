/**
 * Shared browser sandbox for evaluating tag.js in tests.
 *
 * tag.js is a browser IIFE, so each test builds a DOM/Window sandbox,
 * evaluates the tag source inside it, and inspects the payloads handed to
 * sendBeacon. The sandbox never uses real network, storage, or timers > 100ms.
 */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import vm from 'node:vm';

const TAG_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'tag.js');

export const TAG_SOURCE = readFileSync(TAG_PATH, 'utf8');

/**
 * Build a browser-ish sandbox, evaluate tag.js in it, and return handles.
 */
export function loadTag({ scrollHeight = 5000, innerHeight = 1000 } = {}) {
  const sent = [];
  const windowListeners = {};
  const documentListeners = {};
  const store = new Map();

  const document = {
    currentScript: { getAttribute: () => null },
    readyState: 'complete', // init() runs immediately, no DOMContentLoaded wait
    referrer: 'https://referrer.example/',
    title: 'Test Page',
    documentElement: { scrollHeight },
    addEventListener(type, fn) {
      (documentListeners[type] ||= []).push(fn);
    },
  };

  const window = {
    location: {
      href: 'https://site.example/page?utm_source=test',
      search: '?utm_source=test',
      pathname: '/page',
      hostname: 'site.example',
    },
    innerHeight,
    pageYOffset: 0,
    addEventListener(type, fn) {
      (windowListeners[type] ||= []).push(fn);
    },
  };
  window.window = window;

  const sandbox = {
    window,
    document,
    navigator: {
      // Capture the JSON text sent to each beacon URL
      sendBeacon(url, blob) {
        sent.push({ url, payload: JSON.parse(blob.chunks[0]) });
        return true;
      },
    },
    localStorage: {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => store.set(k, String(v)),
      removeItem: (k) => store.delete(k),
    },
    Blob: class Blob {
      constructor(chunks) {
        this.chunks = chunks;
      }
    },
    fetch: () => {
      throw new Error('fetch fallback should not be used when sendBeacon exists');
    },
    URL,
    URLSearchParams,
    // Real timers, but the 30s heartbeat interval must not hold the
    // event loop open after the tests finish
    setTimeout,
    clearTimeout,
    setInterval: (fn, ms) => {
      const id = setInterval(fn, ms);
      id.unref?.();
      return id;
    },
    clearInterval,
  };

  vm.createContext(sandbox);
  vm.runInContext(TAG_SOURCE, sandbox, { filename: 'tag.js' });

  return {
    sent,
    window,
    /** Set the scroll position and fire the tag's scroll listener */
    scrollTo(pageYOffset) {
      window.pageYOffset = pageYOffset;
      for (const fn of windowListeners.scroll || []) fn();
    },
    /** Fire a captured document-level listener (e.g. click) */
    dispatchDocument(type, event) {
      for (const fn of documentListeners[type] || []) fn(event);
    },
  };
}

/** Sleep helper for the scroll throttle's trailing edge */
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
