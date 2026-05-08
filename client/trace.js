/**
 * TRACE - Traffic Recording, Attribution, and Campaign Events
 * Client-side tracking library
 *
 * Features:
 * - Autocapture pageviews on load
 * - Link decoration for session stitching
 * - Cross-domain tracking support (iframe PostMessage, storage bridge)
 * - Heartbeat pings for dwell time
 * - Click tracking on outbound links
 * - Scroll depth tracking
 * - First-party cookie identity and persistence
 * - Privacy: First-party cookies or localStorage only, no third-party cookies
 */

(function(window, document) {
  'use strict';

  // Configuration from script data attributes
  var script = document.currentScript || (function() {
    var scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();

  var CONFIG = {
    collectorUrl: script.getAttribute('data-collector') || '/collect',
    sessionId: null,
    userId: null,
    heartbeatInterval: 30000, // 30 seconds
    scrollThresholds: [25, 50, 75, 90], // percentage thresholds
    sessionTimeout: 1800000, // 30 minutes
    debug: script.getAttribute('data-debug') === 'true',
    storage: script.getAttribute('data-storage') || 'auto', // 'auto', 'cookie', 'localStorage'
    cookieDomain: script.getAttribute('data-cookie-domain') || null,
    cookieSecure: script.getAttribute('data-cookie-secure') === 'true',
    cookieSameSite: script.getAttribute('data-cookie-samesite') || 'Lax',
    crossDomains: parseCrossDomains(script.getAttribute('data-cross-domains'))
  };

  // Storage keys
  var STORAGE_KEYS = {
    SESSION_ID: 'trace_session_id',
    SESSION_START: 'trace_session_start',
    USER_ID: 'trace_user_id',
    PAGE_VIEWED: 'trace_page_viewed_',
    SCROLL_DEPTH: 'trace_scroll_depth_'
  };

  // State tracking
  var state = {
    heartbeatTimer: null,
    maxScrollDepth: 0,
    scrollEventSent: new Set(),
    pageLoadTime: Date.now(),
    url: window.location.href,
    referrer: document.referrer || '',
    iframeOrigins: new Set(), // Origins of iframes we've sent IDs to
    parentOrigin: null // Origin of parent window (if in iframe)
  };

  /**
   * Logging utility
   */
  function debug(message, data) {
    if (CONFIG.debug && window.console) {
      console.log('[TRACE]', message, data || '');
    }
  }

  /**
   * Parse cross-domains configuration
   * @param {string} domainsStr - Comma-separated list of domains
   * @returns {Array|null} Array of normalized domains or null
   */
  function parseCrossDomains(domainsStr) {
    if (!domainsStr) {
      return null;
    }

    var domains = [];
    var parts = domainsStr.split(',');

    for (var i = 0; i < parts.length; i++) {
      var domain = parts[i].trim();
      if (domain) {
        // Normalize domain: remove protocol, path, and port
        try {
          var url = new URL(domain.startsWith('http') ? domain : 'https://' + domain);
          domains.push(url.hostname);
        } catch (e) {
          debug('Invalid cross-domain', domain);
        }
      }
    }

    return domains.length > 0 ? domains : null;
  }

  /**
   * Check if a hostname should be decorated with session ID
   * @param {string} hostname - The link hostname to check
   * @returns {boolean} True if the link should be decorated
   */
  function shouldDecorateLink(hostname) {
    // Same domain
    if (hostname === window.location.hostname) {
      return true;
    }

    // Subdomain
    if (hostname.endsWith('.' + window.location.hostname)) {
      return true;
    }

    // Configured cross-domains
    if (CONFIG.crossDomains) {
      for (var i = 0; i < CONFIG.crossDomains.length; i++) {
        var crossDomain = CONFIG.crossDomains[i];
        if (hostname === crossDomain || hostname.endsWith('.' + crossDomain)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Generate a random UUID v4
   */
  function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0;
      var v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  /**
   * Cookie utility functions
   */
  var CookieUtils = {
    /**
     * Set a cookie
     * @param {string} name - Cookie name
     * @param {string} value - Cookie value
     * @param {number} days - Expiration in days (null for session cookie)
     * @param {string} domain - Cookie domain (optional)
     * @param {boolean} secure - HTTPS only
     * @param {string} sameSite - SameSite attribute
     */
    set: function(name, value, days, domain, secure, sameSite) {
      var expires = '';
      if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        expires = '; expires=' + date.toUTCString();
      }

      var domainStr = '';
      if (domain) {
        domainStr = '; domain=' + domain;
      }

      var secureStr = secure ? '; secure' : '';
      var sameSiteStr = sameSite ? '; samesite=' + sameSite : '';

      document.cookie = name + '=' + encodeURIComponent(value) + expires + domainStr + secureStr + sameSiteStr + '; path=/';
    },

    /**
     * Get a cookie value
     * @param {string} name - Cookie name
     * @returns {string|null} Cookie value or null
     */
    get: function(name) {
      var nameEQ = name + '=';
      var ca = document.cookie.split(';');
      for (var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) === ' ') {
          c = c.substring(1, c.length);
        }
        if (c.indexOf(nameEQ) === 0) {
          return decodeURIComponent(c.substring(nameEQ.length, c.length));
        }
      }
      return null;
    },

    /**
     * Delete a cookie
     * @param {string} name - Cookie name
     * @param {string} domain - Cookie domain (optional)
     */
    remove: function(name, domain) {
      var domainStr = '';
      if (domain) {
        domainStr = '; domain=' + domain;
      }
      document.cookie = name + '=; expires=Thu, 01 Jan 1970 00:00:00 UTC' + domainStr + '; path=/';
    }
  };

  /**
   * PostMessage API for cross-domain iframe communication
   */
  var PostMessageUtils = {
    /**
     * Send session and user IDs to a child iframe
     * @param {HTMLIFrameElement} iframe - The iframe element
     */
    sendToIframe: function(iframe) {
      try {
        if (!iframe.contentWindow) {
          return;
        }

        var iframeOrigin = this.getIframeOrigin(iframe);
        if (!iframeOrigin) {
          return;
        }

        var message = {
          type: 'trace_sync',
          sessionId: CONFIG.sessionId,
          userId: CONFIG.userId,
          version: TRACE.version
        };

        iframe.contentWindow.postMessage(message, iframeOrigin);
        state.iframeOrigins.add(iframeOrigin);
        debug('Sent TRACE IDs to iframe', { origin: iframeOrigin });
      } catch (e) {
        debug('Failed to send to iframe', e);
      }
    },

    /**
     * Get the origin of an iframe
     * @param {HTMLIFrameElement} iframe - The iframe element
     * @returns {string|null} The origin or null
     */
    getIframeOrigin: function(iframe) {
      try {
        if (iframe.src) {
          var url = new URL(iframe.src);
          return url.origin;
        }
      } catch (e) {
        // Invalid URL
      }
      return '*';
    },

    /**
     * Handle incoming PostMessage
     * @param {MessageEvent} event - The message event
     */
    handleMessage: function(event) {
      // Validate message type
      if (!event.data || event.data.type !== 'trace_sync') {
        return;
      }

      // Validate version (basic check)
      if (!event.data.version) {
        return;
      }

      var sourceOrigin = event.origin;
      debug('Received TRACE sync message', { origin: sourceOrigin });

      // Stitch session if provided and different
      if (event.data.sessionId && event.data.sessionId !== CONFIG.sessionId) {
        debug('Session stitched from PostMessage', {
          from: CONFIG.sessionId,
          to: event.data.sessionId,
          origin: sourceOrigin
        });

        if (currentStorage === 'cookie') {
          CookieUtils.set(STORAGE_KEYS.SESSION_ID, event.data.sessionId, null, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
        } else {
          localStorage.setItem(STORAGE_KEYS.SESSION_ID, event.data.sessionId);
        }

        CONFIG.sessionId = event.data.sessionId;
      }

      // Stitch user if provided and different
      if (event.data.userId && event.data.userId !== CONFIG.userId) {
        debug('User stitched from PostMessage', {
          from: CONFIG.userId,
          to: event.data.userId,
          origin: sourceOrigin
        });

        if (currentStorage === 'cookie') {
          CookieUtils.set(STORAGE_KEYS.USER_ID, event.data.userId, 365, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
        } else {
          localStorage.setItem(STORAGE_KEYS.USER_ID, event.data.userId);
        }

        CONFIG.userId = event.data.userId;
      }

      // Record the parent origin for future messages
      state.parentOrigin = sourceOrigin;
    },

    /**
     * Send IDs to parent window (if we're in an iframe)
     */
    sendToParent: function() {
      if (window.self === window.top) {
        // Not in an iframe
        return;
      }

      try {
        var message = {
          type: 'trace_sync',
          sessionId: CONFIG.sessionId,
          userId: CONFIG.userId,
          version: TRACE.version
        };

        // Send to parent origin (or * if unknown)
        var targetOrigin = document.referrer ? new URL(document.referrer).origin : '*';
        window.parent.postMessage(message, targetOrigin);
        debug('Sent TRACE IDs to parent', { origin: targetOrigin });
      } catch (e) {
        debug('Failed to send to parent', e);
      }
    }
  };

  /**
   * Storage Bridge for cross-domain cookie sharing
   * Allows setting cookies on a shared domain via redirect
   */
  var StorageBridge = {
    /**
     * Generate a bridge URL for cross-domain cookie sharing
     * @param {string} bridgeDomain - The domain hosting the bridge endpoint
     * @param {string} targetUrl - The URL to redirect to after setting cookies
     * @returns {string|null} The bridge URL or null
     */
    getBridgeUrl: function(bridgeDomain, targetUrl) {
      if (!bridgeDomain || !targetUrl) {
        debug('Bridge URL requires bridgeDomain and targetUrl');
        return null;
      }

      try {
        var bridgeUrl = new URL('/trace/bridge', 'https://' + bridgeDomain);
        bridgeUrl.searchParams.set('trace_session', CONFIG.sessionId);
        bridgeUrl.searchParams.set('trace_user', CONFIG.userId);
        bridgeUrl.searchParams.set('redirect', targetUrl);
        return bridgeUrl.toString();
      } catch (e) {
        debug('Failed to generate bridge URL', e);
        return null;
      }
    },

    /**
     * Create a bridge link for cross-domain navigation
     * @param {string} href - The target URL
     * @param {string} bridgeDomain - The bridge domain (optional, uses CONFIG.crossDomains if not provided)
     * @returns {string} The bridge URL or original href
     */
    decorateLink: function(href, bridgeDomain) {
      if (!bridgeDomain && CONFIG.crossDomains && CONFIG.crossDomains.length > 0) {
        bridgeDomain = CONFIG.crossDomains[0];
      }

      if (!bridgeDomain) {
        return href;
      }

      try {
        var targetUrl = new URL(href, window.location.href);
        var targetDomain = targetUrl.hostname;

        // Check if target is a cross-domain
        var isCrossDomain = false;
        if (CONFIG.crossDomains) {
          for (var i = 0; i < CONFIG.crossDomains.length; i++) {
            var crossDomain = CONFIG.crossDomains[i];
            if (targetDomain === crossDomain || targetDomain.endsWith('.' + crossDomain)) {
              isCrossDomain = true;
              break;
            }
          }
        }

        if (isCrossDomain && targetDomain !== window.location.hostname) {
          return this.getBridgeUrl(bridgeDomain, href);
        }
      } catch (e) {
        debug('Failed to decorate link with bridge', e);
      }

      return href;
    }
  };

  /**
   * Detect best available storage method
   */
  function detectStorage() {
    if (CONFIG.storage !== 'auto') {
      return CONFIG.storage;
    }

    // Try localStorage first
    try {
      localStorage.setItem('trace_test', '1');
      localStorage.removeItem('trace_test');
      return 'localStorage';
    } catch (e) {
      // localStorage not available (e.g., Safari ITP, private browsing)
      debug('localStorage not available, using cookies');
      return 'cookie';
    }
  }

  // Current storage method
  var currentStorage = detectStorage();

  /**
   * Get or create session ID
   */
  function getOrCreateSessionId() {
    var sessionId, sessionStart;

    // Try to get from current storage method
    if (currentStorage === 'cookie') {
      sessionId = CookieUtils.get(STORAGE_KEYS.SESSION_ID);
      sessionStart = CookieUtils.get(STORAGE_KEYS.SESSION_START);

      // Check if session has expired
      if (sessionId && sessionStart) {
        var elapsed = Date.now() - parseInt(sessionStart, 10);
        if (elapsed > CONFIG.sessionTimeout) {
          debug('Session expired, creating new session');
          CookieUtils.remove(STORAGE_KEYS.SESSION_ID, CONFIG.cookieDomain);
          CookieUtils.remove(STORAGE_KEYS.SESSION_START, CONFIG.cookieDomain);
          sessionId = null;
        }
      }

      if (!sessionId) {
        sessionId = generateUUID();
        // Session cookie (expires when browser closes) or 30 minutes
        CookieUtils.set(STORAGE_KEYS.SESSION_ID, sessionId, null, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
        CookieUtils.set(STORAGE_KEYS.SESSION_START, Date.now().toString(), null, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
        debug('New session created (cookie)', sessionId);
      }
    } else {
      // localStorage fallback
      sessionId = localStorage.getItem(STORAGE_KEYS.SESSION_ID);
      sessionStart = localStorage.getItem(STORAGE_KEYS.SESSION_START);

      // Check if session has expired
      if (sessionId && sessionStart) {
        var elapsed = Date.now() - parseInt(sessionStart, 10);
        if (elapsed > CONFIG.sessionTimeout) {
          debug('Session expired, creating new session');
          localStorage.removeItem(STORAGE_KEYS.SESSION_ID);
          localStorage.removeItem(STORAGE_KEYS.SESSION_START);
          sessionId = null;
        }
      }

      if (!sessionId) {
        sessionId = generateUUID();
        localStorage.setItem(STORAGE_KEYS.SESSION_ID, sessionId);
        localStorage.setItem(STORAGE_KEYS.SESSION_START, Date.now().toString());
        debug('New session created (localStorage)', sessionId);
      }
    }

    CONFIG.sessionId = sessionId;
    return sessionId;
  }

  /**
   * Get or create user ID
   */
  function getOrCreateUserId() {
    var userId;

    if (currentStorage === 'cookie') {
      userId = CookieUtils.get(STORAGE_KEYS.USER_ID);

      if (!userId) {
        userId = generateUUID();
        // User ID persists for 1 year
        CookieUtils.set(STORAGE_KEYS.USER_ID, userId, 365, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
        debug('New user ID created (cookie)', userId);
      }
    } else {
      // localStorage fallback
      userId = localStorage.getItem(STORAGE_KEYS.USER_ID);

      if (!userId) {
        userId = generateUUID();
        localStorage.setItem(STORAGE_KEYS.USER_ID, userId);
        debug('New user ID created (localStorage)', userId);
      }
    }

    CONFIG.userId = userId;
    return userId;
  }

  /**
   * Send event to collector
   */
  function sendEvent(type, data) {
    var payload = {
      type: type,
      url: window.location.href,
      ts: new Date().toISOString(),
      session_id: CONFIG.sessionId,
      user_id: CONFIG.userId,
      referrer: state.referrer
    };

    // Merge additional data
    for (var key in data) {
      if (data.hasOwnProperty(key)) {
        payload[key] = data[key];
      }
    }

    // Send using navigator.sendBeacon for reliability during page unload
    if (navigator.sendBeacon) {
      var blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
      try {
        navigator.sendBeacon(CONFIG.collectorUrl, blob);
        debug('Event sent via sendBeacon', payload);
        return;
      } catch (e) {
        debug('sendBeacon failed, falling back to fetch', e);
      }
    }

    // Fallback to fetch
    fetch(CONFIG.collectorUrl, {
      method: 'POST',
      body: JSON.stringify(payload),
      headers: {
        'Content-Type': 'application/json'
      },
      keepalive: true
    }).catch(function(err) {
      debug('Failed to send event', err);
    });
  }

  /**
   * Capture pageview event
   */
  function capturePageview() {
    // Check if this page was already viewed (for SPA navigation)
    var pageKey = STORAGE_KEYS.PAGE_VIEWED + window.location.pathname;
    var lastViewed = sessionStorage.getItem(pageKey);

    // Extract URL parameters for campaign tracking
    var urlParams = {};
    var searchParams = new URLSearchParams(window.location.search);
    searchParams.forEach(function(value, key) {
      urlParams[key] = value;
    });

    var pageviewData = {
      title: document.title,
      path: window.location.pathname,
      search: window.location.search,
      params: urlParams,
      referrer: state.referrer,
      page_view_key: pageKey
    };

    // Mark page as viewed
    sessionStorage.setItem(pageKey, Date.now().toString());

    sendEvent('pageview', pageviewData);
    debug('Pageview captured', pageviewData);
  }

  /**
   * Start heartbeat for dwell time tracking
   */
  function startHeartbeat() {
    if (state.heartbeatTimer) {
      clearInterval(state.heartbeatTimer);
    }

    state.heartbeatTimer = setInterval(function() {
      var dwellTime = Date.now() - state.pageLoadTime;
      sendEvent('dwell', {
        dwell_time: dwellTime,
        dwell_time_seconds: Math.floor(dwellTime / 1000)
      });
      debug('Heartbeat sent', { dwellTime: dwellTime });
    }, CONFIG.heartbeatInterval);

    debug('Heartbeat started', { interval: CONFIG.heartbeatInterval });
  }

  /**
   * Track scroll depth
   */
  function trackScrollDepth() {
    var calculateScrollDepth = function() {
      var scrollTop = window.pageYOffset || document.documentElement.scrollTop;
      var docHeight = document.documentElement.scrollHeight;
      var winHeight = window.innerHeight;
      var scrollPercent = Math.floor((scrollTop / (docHeight - winHeight)) * 100);

      if (scrollPercent > state.maxScrollDepth) {
        state.maxScrollDepth = scrollPercent;
      }

      // Check thresholds
      CONFIG.scrollThresholds.forEach(function(threshold) {
        var key = 'scroll_' + threshold;
        if (scrollPercent >= threshold && !state.scrollEventSent.has(key)) {
          state.scrollEventSent.add(key);
          sendEvent('scroll', {
            scroll_depth: threshold,
            max_scroll_depth: state.maxScrollDepth
          });
          debug('Scroll threshold reached', { threshold: threshold });
        }
      });

      // Store max scroll depth for potential form submission tracking
      sessionStorage.setItem(STORAGE_KEYS.SCROLL_DEPTH + window.location.pathname, state.maxScrollDepth.toString());
    };

    // Throttled scroll handler
    var scrollTimeout;
    window.addEventListener('scroll', function() {
      if (scrollTimeout) {
        clearTimeout(scrollTimeout);
      }
      scrollTimeout = setTimeout(calculateScrollDepth, 100);
    });

    debug('Scroll tracking initialized');
  }

  /**
   * Track clicks on links
   */
  function trackClicks() {
    document.addEventListener('click', function(e) {
      var target = e.target;
      var link = target.closest('a');

      if (!link) {
        return;
      }

      var href = link.getAttribute('href');
      if (!href || href.startsWith('#') || href.startsWith('javascript:')) {
        return;
      }

      // Determine if outbound link
      var isOutbound = false;
      try {
        var linkUrl = new URL(href, window.location.href);
        isOutbound = linkUrl.hostname !== window.location.hostname;
      } catch (err) {
        debug('Invalid URL', href);
      }

      var clickData = {
        link_url: href,
        link_text: link.textContent.trim().substring(0, 100),
        link_id: link.id || '',
        outbound: isOutbound,
        x: e.clientX,
        y: e.clientY
      };

      sendEvent('click', clickData);
      debug('Click captured', clickData);
    }, true);

    debug('Click tracking initialized');
  }

  /**
   * Decorate links with session ID and user ID for cross-site tracking
   */
  function decorateLinks() {
    var links = document.querySelectorAll('a[href]');
    var sessionParam = 'trace_session=' + CONFIG.sessionId;
    var userParam = 'trace_user=' + CONFIG.userId;

    for (var i = 0; i < links.length; i++) {
      var link = links[i];
      var href = link.getAttribute('href');

      // Skip if already decorated or has special attributes
      if (!href || href.indexOf('trace_session=') > -1 || link.getAttribute('data-trace-skip') !== null) {
        continue;
      }

      // Decorate links to same domain, subdomains, or configured cross-domains
      try {
        var linkUrl = new URL(href, window.location.href);
        if (shouldDecorateLink(linkUrl.hostname)) {
          var separator = href.indexOf('?') > -1 ? '&' : '?';
          link.setAttribute('href', href + separator + sessionParam + '&' + userParam);
        }
      } catch (err) {
        // Invalid URL, skip
      }
    }

    debug('Links decorated with session ID and user ID', {
      crossDomains: CONFIG.crossDomains
    });
  }

  /**
   * Send TRACE IDs to all child iframes
   */
  function syncToIframes() {
    var iframes = document.querySelectorAll('iframe');
    for (var i = 0; i < iframes.length; i++) {
      PostMessageUtils.sendToIframe(iframes[i]);
    }
    debug('Synced TRACE IDs to iframes', { count: iframes.length });
  }

  /**
   * Handle visibility change (tab switch)
   */
  function handleVisibilityChange() {
    if (document.hidden) {
      // Page hidden, send dwell event
      var dwellTime = Date.now() - state.pageLoadTime;
      sendEvent('dwell', {
        dwell_time: dwellTime,
        dwell_time_seconds: Math.floor(dwellTime / 1000),
        hidden: true
      });
    } else {
      // Page visible again
      state.pageLoadTime = Date.now();
    }
  }

  /**
   * Handle page unload
   */
  function handleUnload() {
    // Send final dwell event
    var dwellTime = Date.now() - state.pageLoadTime;
    sendEvent('dwell', {
      dwell_time: dwellTime,
      dwell_time_seconds: Math.floor(dwellTime / 1000),
      unload: true
    });

    // Clear heartbeat
    if (state.heartbeatTimer) {
      clearInterval(state.heartbeatTimer);
    }
  }

  /**
   * Initialize TRACE tracking
   */
  function init() {
    debug('Initializing TRACE');

    // Set up PostMessage listener for iframe communication
    window.addEventListener('message', function(event) {
      PostMessageUtils.handleMessage(event);
    });

    // Get or create session and user IDs
    getOrCreateSessionId();
    getOrCreateUserId();

    // Check for session ID in URL (from decorated link)
    var urlParams = new URLSearchParams(window.location.search);
    var linkSessionId = urlParams.get('trace_session');
    if (linkSessionId && linkSessionId !== CONFIG.sessionId) {
      // Cross-site session stitching
      debug('Session stitched from link', { from: CONFIG.sessionId, to: linkSessionId });

      if (currentStorage === 'cookie') {
        CookieUtils.set(STORAGE_KEYS.SESSION_ID, linkSessionId, null, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
      } else {
        localStorage.setItem(STORAGE_KEYS.SESSION_ID, linkSessionId);
      }

      CONFIG.sessionId = linkSessionId;
    }

    // Check for user ID in URL (from decorated link)
    var linkUserId = urlParams.get('trace_user');
    if (linkUserId && linkUserId !== CONFIG.userId) {
      // Cross-site user stitching
      debug('User stitched from link', { from: CONFIG.userId, to: linkUserId });

      if (currentStorage === 'cookie') {
        CookieUtils.set(STORAGE_KEYS.USER_ID, linkUserId, 365, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
      } else {
        localStorage.setItem(STORAGE_KEYS.USER_ID, linkUserId);
      }

      CONFIG.userId = linkUserId;
    }

    // Send IDs to parent window if we're in an iframe
    PostMessageUtils.sendToParent();

    // Capture initial pageview
    capturePageview();

    // Start tracking features
    startHeartbeat();
    trackScrollDepth();
    trackClicks();
    decorateLinks();
    syncToIframes();

    // Watch for dynamically added iframes
    var observer = new MutationObserver(function(mutations) {
      mutations.forEach(function(mutation) {
        if (mutation.type === 'childList') {
          mutation.addedNodes.forEach(function(node) {
            if (node.nodeName === 'IFRAME') {
              PostMessageUtils.sendToIframe(node);
            } else if (node.querySelectorAll) {
              var iframes = node.querySelectorAll('iframe');
              for (var i = 0; i < iframes.length; i++) {
                PostMessageUtils.sendToIframe(iframes[i]);
              }
            }
          });
        }
      });
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true
    });

    // Event listeners
    document.addEventListener('visibilitychange', handleVisibilityChange);
    window.addEventListener('beforeunload', handleUnload);

    // Handle SPA navigation
    var pushState = history.pushState;
    var replaceState = history.replaceState;

    history.pushState = function() {
      pushState.apply(history, arguments);
      TRACE.pageview();
    };

    history.replaceState = function() {
      replaceState.apply(history, arguments);
      TRACE.pageview();
    };

    window.addEventListener('popstate', function() {
      TRACE.pageview();
    });

    debug('TRACE initialized', {
      sessionId: CONFIG.sessionId,
      userId: CONFIG.userId,
      collectorUrl: CONFIG.collectorUrl,
      inIframe: window.self !== window.top
    });
  }

  // Public API
  var TRACE = {
    version: '1.2.0',

    /**
     * Send a custom event
     * @param {string} type - Event type
     * @param {object} data - Event data
     */
    collect: function(type, data) {
      if (typeof type === 'object') {
        data = type;
        type = 'custom';
      }
      sendEvent(type || 'custom', data || {});
    },

    /**
     * Manually capture a pageview
     */
    pageview: function() {
      state.url = window.location.href;
      state.referrer = document.referrer || '';
      state.pageLoadTime = Date.now();
      state.maxScrollDepth = 0;
      state.scrollEventSent.clear();
      capturePageview();
    },

    /**
     * Identify user with custom ID
     * @param {string} userId - Custom user ID
     * @param {object} options - Optional parameters
     * @param {number} options.expires - Days until cookie expires (cookie storage only)
     */
    identify: function(userId, options) {
      if (userId) {
        options = options || {};
        var expires = options.expires !== undefined ? options.expires : 365;

        if (currentStorage === 'cookie') {
          CookieUtils.set(STORAGE_KEYS.USER_ID, userId, expires, CONFIG.cookieDomain, CONFIG.cookieSecure, CONFIG.cookieSameSite);
        } else {
          localStorage.setItem(STORAGE_KEYS.USER_ID, userId);
        }

        CONFIG.userId = userId;
        sendEvent('identify', { user_id: userId });
        debug('User identified', { userId: userId, storage: currentStorage });
      }
    },

    /**
     * Get current session ID
     */
    getSessionId: function() {
      return CONFIG.sessionId;
    },

    /**
     * Get current user ID
     */
    getUserId: function() {
      return CONFIG.userId;
    },

    /**
     * Reset session (create new session)
     */
    reset: function() {
      if (currentStorage === 'cookie') {
        CookieUtils.remove(STORAGE_KEYS.SESSION_ID, CONFIG.cookieDomain);
        CookieUtils.remove(STORAGE_KEYS.SESSION_START, CONFIG.cookieDomain);
      } else {
        localStorage.removeItem(STORAGE_KEYS.SESSION_ID);
        localStorage.removeItem(STORAGE_KEYS.SESSION_START);
      }
      getOrCreateSessionId();
      debug('Session reset', { storage: currentStorage });
    },

    /**
     * Get a bridge URL for cross-domain cookie sharing
     * @param {string} bridgeDomain - The domain hosting the bridge endpoint
     * @param {string} targetUrl - The URL to redirect to after setting cookies
     * @returns {string|null} The bridge URL or null
     */
    getBridgeUrl: function(bridgeDomain, targetUrl) {
      return StorageBridge.getBridgeUrl(bridgeDomain, targetUrl);
    },

    /**
     * Decorate a link with session ID and user ID
     * @param {string} href - The link URL
     * @returns {string} The decorated URL
     */
    decorateLink: function(href) {
      if (!href) {
        return href;
      }
      var sessionParam = 'trace_session=' + CONFIG.sessionId;
      var userParam = 'trace_user=' + CONFIG.userId;
      var separator = href.indexOf('?') > -1 ? '&' : '?';
      return href + separator + sessionParam + '&' + userParam;
    },

    /**
     * Send TRACE IDs to a specific iframe
     * @param {HTMLIFrameElement} iframe - The iframe element
     */
    syncToIframe: function(iframe) {
      PostMessageUtils.sendToIframe(iframe);
    },

    /**
     * Send TRACE IDs to parent window (if in iframe)
     */
    syncToParent: function() {
      PostMessageUtils.sendToParent();
    },

    /**
     * Get current configuration
     * @returns {object} Current configuration
     */
    getConfig: function() {
      return {
        collectorUrl: CONFIG.collectorUrl,
        storage: currentStorage,
        cookieDomain: CONFIG.cookieDomain,
        crossDomains: CONFIG.crossDomains,
        debug: CONFIG.debug
      };
    }
  };

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Export to global scope
  window.TRACE = TRACE;

})(window, document);
