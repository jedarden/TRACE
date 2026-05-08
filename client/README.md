# TRACE Client Integration

**Traffic Recording, Attribution, and Campaign Events**

Client-side tracking library for TRACE. Includes a JavaScript tag for comprehensive event tracking and a pixel tag for simple pageview tracking.

---

## Quick Start

### JavaScript Tag (Recommended)

Add this to your website's `<head>` section:

```html
<script src="https://your-domain.com/trace.js"
        data-collector="https://your-domain.com/collect"></script>
```

### Pixel Tag (Simple)

For basic pageview tracking only:

```html
<img src="https://your-domain.com/collect?url=PAGE_URL&type=pageview"
     width="1" height="1" alt="" style="display:none">
```

---

## Features

### JavaScript Tag Features

- **Autocapture Pageviews** - Automatically tracks pageviews on load and SPA navigation
- **Dwell Time Tracking** - Heartbeat pings every 30 seconds measure time on page
- **Scroll Depth** - Tracks scroll thresholds at 25%, 50%, 75%, 90%
- **Click Tracking** - Captures all link clicks with coordinates
- **Session Stitching** - Decorates links with session ID for cross-site tracking
- **Cross-Domain Tracking** - Track users across multiple domains and subdomains
- **First-Party Cookies** - Cookie-based identity and persistence with localStorage fallback
- **Privacy-First** - First-party cookies or localStorage only, no third-party cookies
- **SPA Support** - Works with React, Vue, Angular, and other SPAs

### Pixel Tag Features

- Simple pageview tracking
- Works with basic HTML
- No JavaScript required
- Backward compatible with legacy systems

---

## Configuration

### Data Attributes

Configure the JavaScript tag using data attributes:

| Attribute | Default | Description |
|-----------|---------|-------------|
| `data-collector` | `/collect` | Collector endpoint URL |
| `data-debug` | `false` | Enable console logging (`"true"` to enable) |
| `data-storage` | `'auto'` | Storage method: `'auto'`, `'cookie'`, or `'localStorage'` |
| `data-cookie-domain` | `null` | Domain scope for cookies (e.g., `.example.com`) |
| `data-cookie-secure` | `false` | Set Secure flag for HTTPS-only cookies |
| `data-cookie-samesite` | `'Lax'` | SameSite attribute: `'Strict'`, `'Lax'`, or `'None'` |
| `data-cross-domains` | `null` | Comma-separated list of domains for cross-domain tracking |

Example:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-debug="true"
        data-cross-domains="example.com,another-domain.com"></script>
```

---

## Storage Options

TRACE supports multiple storage methods for session and user identity:

### Auto Detection (Recommended)

Automatically detects the best available storage method:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-storage="auto"></script>
```

**Behavior:**
1. Tries `localStorage` first
2. Falls back to cookies if localStorage fails (Safari ITP, private browsing)
3. Provides maximum compatibility across browsers and scenarios

### Cookie Storage

Explicitly use first-party cookies:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-storage="cookie"
        data-cookie-domain=".example.com"></script>
```

**Benefits:**
- Survives Safari's Intelligent Tracking Prevention (ITP)
- Works in private browsing mode
- Accessible server-side (for future features)
- Better cross-tab reliability

### Local Storage Only

Force localStorage (original behavior):

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-storage="localStorage"></script>
```

### Cookie Configuration

For HTTPS sites, configure secure cookies:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-storage="cookie"
        data-cookie-secure="true"
        data-cookie-samesite="Strict"></script>
```

### Cross-Domain Cookies

Share identity across subdomains:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-storage="cookie"
        data-cookie-domain=".example.com"></script>
```

This allows `www.example.com` and `shop.example.com` to share the same user identity.

---

## Public API

The JavaScript tag exposes a global `TRACE` object:

### `TRACE.collect(type, data)`

Send a custom event:

```javascript
// With event type
TRACE.collect('signup', { plan: 'premium' });

// Shorthand - defaults to type 'custom'
TRACE.collect({ category: 'engagement', action: 'video-play' });
```

### `TRACE.identify(userId, options)`

Identify a user with your own ID:

```javascript
// Basic usage (1 year expiration for cookies)
TRACE.identify('user-12345');

// Custom expiration (30 days)
TRACE.identify('user-12345', { expires: 30 });
```

**Options:**
- `expires`: Days until cookie expiration (only for cookie storage, default: 365)

### `TRACE.pageview()`

Manually trigger a pageview event (useful for SPAs):

```javascript
TRACE.pageview();
```

### `TRACE.getSessionId()`

Get the current session ID:

```javascript
var sessionId = TRACE.getSessionId();
```

### `TRACE.getUserId()`

Get the current user ID:

```javascript
var userId = TRACE.getUserId();
```

### `TRACE.reset()`

Create a new session:

```javascript
TRACE.reset();
```

---

## Event Types

TRACE automatically sends these event types:

| Type | Description | Data Fields |
|------|-------------|-------------|
| `pageview` | Page load and navigation | `title`, `path`, `search`, `params`, `referrer` |
| `dwell` | Heartbeat every 30s | `dwell_time`, `dwell_time_seconds` |
| `scroll` | Scroll threshold reached | `scroll_depth`, `max_scroll_depth` |
| `click` | Link clicked | `link_url`, `link_text`, `link_id`, `outbound`, `x`, `y` |
| `identify` | User identified | `user_id` |
| `custom` | Custom events | Your custom fields |

All events include:
- `type` - Event type
- `url` - Current page URL
- `ts` - Timestamp (ISO 8601)
- `session_id` - Session UUID
- `user_id` - User UUID
- `referrer` - Referrer URL

---

## Session Management

### Session ID

- Stored in first-party cookies or localStorage (configurable)
- Cookie name: `trace_session_id`
- Persists for the browser session (expires when browser closes)
- Expires after 30 minutes of inactivity
- Automatically created on first visit

### User ID

- Stored in first-party cookies or localStorage (configurable)
- Cookie name: `trace_user_id`
- Persists for 1 year (cookies) or indefinitely (localStorage)
- Random UUID until manually set via `TRACE.identify()`
- Used for cross-session user attribution

### Storage Behavior

| Storage Method | Session ID | User ID | Best For |
|----------------|------------|---------|----------|
| `auto` | Cookie or localStorage | Cookie or localStorage | Maximum compatibility |
| `cookie` | Session cookie | 1 year | Safari ITP, cross-domain |
| `localStorage` | Session storage | Persistent | Legacy support |

### Session Stitching

Links are automatically decorated with `trace_session` parameter for cross-site tracking:

```html
<a href="https://other-site.com/page?trace_session=abc-123">Link</a>
```

When a user clicks a decorated link, the destination page inherits the session ID.

#### Cross-Domain Tracking

To track users across multiple domains, configure the `data-cross-domains` attribute:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"
        data-cross-domains="example.com,shop.example.com,blog.example.com"></script>
```

This will:
1. Decorate links to the configured domains with the session ID
2. Allow the session to persist across domain boundaries
3. Enable user journey tracking across multiple properties

**Use cases:**
- Track users from your main site to a checkout domain
- Follow users across related microsites
- Measure cross-domain conversions and funnels

**Note:** For cross-domain tracking to work, the TRACE JavaScript tag must be installed on all domains with the same `data-cross-domains` configuration.

---

## Campaign Tracking

TRACE automatically captures UTM parameters and other query params:

```javascript
// URL: https://example.com/page?utm_source=taboola&utm_campaign=c123&item=i456

// Captured params:
{
  utm_source: "taboola",
  utm_campaign: "c123",
  item: "i456"
}
```

Supported ad network parameters:
- **Taboola**: `tb_item`, `tb_image`, `tb_headline`
- **Outbrain**: `ob_item`, `ob_creative`
- **MGID**: `mg_id`, `mg_title`, `mg_image`
- **RevContent**: `rc_id`, `rc_title`, `rc_thumb`

---

## Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- Mobile browsers (iOS Safari, Chrome Mobile)

**Requirements:**
- `localStorage`
- `sessionStorage`
- `navigator.sendBeacon()` or `fetch` API
- `URL` and `URLSearchParams` APIs

**IE11 Support:**
Requires polyfills for `URL`, `URLSearchParams`, and `Promise`.

---

## Privacy

TRACE is designed with privacy in mind:

- **No third-party cookies** - All data stored in first-party localStorage
- **No cross-site tracking** - Without explicit link decoration
- **Random UUIDs** - Session and user IDs are random, not derived from personal info
- **No PII by default** - Does not capture names, emails, or other personal data
- **GDPR compliant** - Data stored locally, user can clear via localStorage

---

## Demos

- **[demo.html](demo.html)** - Interactive JavaScript tag demo
- **[pixel-example.html](pixel-example.html)** - Pixel tag documentation and examples

---

## Development

### File Structure

```
client/
├── trace.js           # Main JavaScript tracking library
├── demo.html          # Interactive demo and documentation
├── pixel-example.html # Pixel tag examples
└── README.md          # This file
```

### Testing Locally

1. Start a local server:
   ```bash
   python3 -m http.server 8000
   ```

2. Open demo page:
   ```
   http://localhost:8000/demo.html
   ```

3. Enable debug mode to see console logs:
   ```html
   <script src="trace.js" data-debug="true"></script>
   ```

---

## Deployment

### Serve trace.js

Host `trace.js` on your web server or CDN:

```nginx
# nginx example
location /trace.js {
    root /var/www/TRACE/client;
    expires 1d;
    add_header Cache-Control "public, immutable";
}
```

### Configure Collector Endpoint

Update `data-collector` to point to your TRACE collector:

```html
<script src="trace.js"
        data-collector="https://trace.example.com/collect"></script>
```

---

## Troubleshooting

### Events Not Appearing

1. Check browser console for errors (enable debug mode)
2. Verify collector endpoint is accessible
3. Check network tab for failed requests
4. Ensure ad blocker is not blocking requests

### Session Not Persisting

1. Check that localStorage is enabled
2. Verify browser privacy settings
3. Check for same-origin policy issues

### SPA Navigation Not Tracked

1. Call `TRACE.pageview()` manually after route changes
2. Or ensure your router uses `history.pushState()`

---

## License

MIT

---

## Support

For issues and questions, please refer to the main [TRACE README](../README.md).
