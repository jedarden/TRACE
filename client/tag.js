/*!
 * TRACE - Minimal async tracking tag
 * @version 1.1.0
 *
 * Features:
 * - Pageview on DOMContentLoaded (captures all query params including UTM)
 * - Dwell heartbeat every 30 seconds
 * - Click tracking on outbound links
 * - Scroll depth tracking (25/50/75/100% thresholds)
 * - Conversion tracking with revenue via TRACE.conversion()
 * - Sends POST to /e endpoint with JSON payload
 * - Async, non-blocking, <4KB minified
 *
 * Usage:
 * <script src="tag.min.js" data-collector="/e"></script>
 */

(function() {
  'use strict';

  // Get script element and configuration
  var script = document.currentScript || (function() {
    var scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();

  var collectorUrl = script.getAttribute('data-collector') || '/e';

  // Generate UUID v4
  function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0;
      var v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  // Get or create session ID
  var sessionId = (function() {
    var sid = generateUUID();
    try {
      localStorage.setItem('t_sid', sid);
    } catch (e) {}
    return sid;
  })();

  // Get or create user ID
  var userId = (function() {
    var uid = generateUUID();
    try {
      localStorage.setItem('t_uid', uid);
    } catch (e) {}
    return uid;
  })();

  // Page view ID (unique per page load)
  var pageViewId = generateUUID();

  // Page load timestamp
  var pageLoadTime = Date.now();

  // Heartbeat timer reference
  var heartbeatTimer = null;

  // URL params cache
  var urlParams = {};

  /**
   * Send event to collector
   * @param {string} type - Event type (pageview, dwell, click, scroll)
   * @param {object} data - Event data
   */
  function sendEvent(type, data) {
    try {
      var payload = {
        type: type,
        url: window.location.href,
        ts: new Date().toISOString(),
        sid: sessionId,
        uid: userId,
        pv: pageViewId,
        referrer: document.referrer
      };

      // Merge additional data
      for (var key in data) {
        if (data.hasOwnProperty(key)) {
          payload[key] = data[key];
        }
      }

      var blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });

      // Use sendBeacon for reliability during page unload
      if (navigator.sendBeacon) {
        navigator.sendBeacon(collectorUrl, blob);
      } else {
        // Fallback to fetch with keepalive
        fetch(collectorUrl, {
          method: 'POST',
          body: blob,
          keepalive: true
        });
      }
    } catch (e) {
      // Silently fail to not break page
    }
  }

  /**
   * Capture pageview event
   */
  function capturePageview() {
    // Extract all URL parameters including UTM
    new URLSearchParams(window.location.search).forEach(function(value, key) {
      urlParams[key] = value;
    });

    var pageviewData = {
      params: urlParams,
      title: document.title,
      path: window.location.pathname,
      search: window.location.search,
      // Explicit UTM fields for easy access
      utm_source: urlParams.utm_source,
      utm_medium: urlParams.utm_medium,
      utm_campaign: urlParams.utm_campaign,
      utm_term: urlParams.utm_term,
      utm_content: urlParams.utm_content,
      // Ad network macros - raw values stored alongside normalized names
      adn: {
        // Taboola
        taboola: {
          campaign_id: urlParams.tb_cli_campaign,
          click_id: urlParams.tb_click_id,
          _raw: {
            tb_cli_campaign: urlParams.tb_cli_campaign,
            tb_click_id: urlParams.tb_click_id
          }
        },
        // Outbrain
        outbrain: {
          orig_url: urlParams.obOrigUrl,
          params: urlParams.outbrain_params,
          _raw: {
            obOrigUrl: urlParams.obOrigUrl,
            outbrain_params: urlParams.outbrain_params
          }
        },
        // MGID
        mgid: {
          trid: urlParams.trid,
          utm_content: urlParams.utm_content,
          _raw: {
            trid: urlParams.trid,
            utm_content: urlParams.utm_content
          }
        },
        // RevContent
        revcontent: {
          uuid: urlParams.rc_uuid,
          widget_id: urlParams.widget_id,
          _raw: {
            rc_uuid: urlParams.rc_uuid,
            widget_id: urlParams.widget_id
          }
        }
      }
    };

    sendEvent('pageview', pageviewData);
  }

  /**
   * Send dwell heartbeat
   */
  function sendHeartbeat() {
    var dwellTime = Date.now() - pageLoadTime;
    sendEvent('dwell', {
      dwell: dwellTime,
      dwell_sec: Math.floor(dwellTime / 1000)
    });
  }

  // Scroll depth tracking: fire a scroll event once per threshold reached
  var scrollThresholds = [25, 50, 75, 100];
  var sentThresholds = {};
  var maxScrollDepth = 0;
  var scrollTimer = null;

  /**
   * Compute current scroll depth and emit a scroll event for each
   * newly-reached threshold
   */
  function computeScrollDepth() {
    scrollTimer = null;

    var doc = document.documentElement;
    var scrollable = doc.scrollHeight - window.innerHeight;

    // A page shorter than the viewport counts as fully viewed
    var pct = scrollable > 0
      ? Math.round((window.pageYOffset / scrollable) * 100)
      : 100;
    if (pct > 100) {
      pct = 100;
    }

    if (pct > maxScrollDepth) {
      maxScrollDepth = pct;
    }

    for (var i = 0; i < scrollThresholds.length; i++) {
      var threshold = scrollThresholds[i];
      if (maxScrollDepth >= threshold && !sentThresholds[threshold]) {
        sentThresholds[threshold] = true;
        sendEvent('scroll', {
          scroll_depth: threshold,
          max_scroll_depth: maxScrollDepth
        });
      }
    }
  }

  /**
   * Track scroll depth (trailing-edge throttled)
   */
  function trackScrollDepth() {
    window.addEventListener('scroll', function() {
      if (scrollTimer) {
        clearTimeout(scrollTimer);
      }
      scrollTimer = setTimeout(computeScrollDepth, 100);
    });
  }

  /**
   * Initialize tracking
   */
  function init() {
    // Capture initial pageview
    capturePageview();

    // Start heartbeat (every 30 seconds)
    heartbeatTimer = setInterval(sendHeartbeat, 30000);

    // Track scroll depth thresholds
    trackScrollDepth();
  }

  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Track outbound link clicks
  document.addEventListener('click', function(e) {
    var target = e.target;
    var link = target.closest('a');

    if (!link || !link.href) {
      return;
    }

    var href = link.href;

    // Skip anchors and javascript links
    if (href.startsWith('#') || href.startsWith('javascript:')) {
      return;
    }

    // Check if outbound link
    var isOutbound = (function(url) {
      try {
        return new URL(url, window.location.href).hostname !== window.location.hostname;
      } catch (e) {
        return false;
      }
    })(href);

    if (isOutbound) {
      sendEvent('click', {
        url: href,
        text: link.textContent.trim().substring(0, 100),
        outbound: true,
        x: e.clientX,
        y: e.clientY
      });
    }
  }, true);

  // Send final dwell event on page unload
  window.addEventListener('beforeunload', function() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
    }
    var dwellTime = Date.now() - pageLoadTime;
    sendEvent('dwell', {
      dwell: dwellTime,
      dwell_sec: Math.floor(dwellTime / 1000),
      unload: true
    });
  });

  /**
   * Track a conversion event (purchase, signup, lead, ...).
   *
   * The event type is always 'conversion' — that is what the attribution
   * queries count (type = 'conversion'). What kind of conversion it was
   * travels in conversion_type, and revenue rides along in params:
   *
   *   TRACE.conversion({ conversion_type: 'purchase', revenue: 49.99 });
   *   TRACE.conversion('signup');                       // no revenue
   *   TRACE.conversion({ type: 'lead', revenue: 10 });  // type becomes conversion_type
   *
   * Any extra keys (currency, order_id, ...) are passed through as params.
   *
   * @param {object|string} [options] - Conversion details, or just the type
   */
  function trackConversion(options) {
    if (typeof options === 'string') {
      options = { conversion_type: options };
    }
    options = options || {};

    if (!options.conversion_type) {
      options.conversion_type = options.type || 'conversion';
    }
    // Keep the payload type stable: sendEvent merges data over the envelope,
    // and a stray type key would reclassify the event.
    delete options.type;

    sendEvent('conversion', options);
  }

  // Public API
  window.TRACE = window.TRACE || {};
  window.TRACE.conversion = trackConversion;

})();
