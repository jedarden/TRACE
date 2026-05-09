/*!
 * TRACE - Minimal async tracking tag
 * @version 1.0.0
 *
 * Features:
 * - Pageview on DOMContentLoaded (captures all query params including UTM)
 * - Dwell heartbeat every 30 seconds
 * - Click tracking on outbound links
 * - Sends POST to /e endpoint with JSON payload
 * - Async, non-blocking, <2KB
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
   * @param {string} type - Event type (pageview, dwell, click)
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

  /**
   * Initialize tracking
   */
  function init() {
    // Capture initial pageview
    capturePageview();

    // Start heartbeat (every 30 seconds)
    heartbeatTimer = setInterval(sendHeartbeat, 30000);
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

})();
