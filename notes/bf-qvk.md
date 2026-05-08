# First-Party Cookie Identity and Persistence (bf-qvk)

## Summary

Implemented first-party cookie-based identity and persistence for the TRACE JavaScript client library. The implementation provides automatic fallback between localStorage and cookies based on availability and configuration.

## Implementation

### 1. Cookie Utility Functions (`client/trace.js`)

Added `CookieUtils` object with three methods:

- **set(name, value, days, domain, secure, sameSite)**: Sets a cookie with optional expiration
- **get(name)**: Retrieves a cookie value
- **remove(name, domain)**: Deletes a cookie

### 2. Storage Detection

Added `detectStorage()` function that:
- Returns configured storage method if explicitly set
- Attempts localStorage first (in 'auto' mode)
- Falls back to cookies if localStorage fails (Safari ITP, private browsing)

### 3. Configuration Options

New data attributes for the TRACE script tag:

| Attribute | Default | Description |
|-----------|---------|-------------|
| `data-storage` | `'auto'` | Storage method: 'auto', 'cookie', or 'localStorage' |
| `data-cookie-domain` | `null` | Domain scope for cookies (e.g., '.example.com') |
| `data-cookie-secure` | `false` | Set Secure flag (HTTPS only) |
| `data-cookie-samesite` | `'Lax'` | SameSite attribute: 'Strict', 'Lax', or 'None' |

### 4. Identity Storage

**Session ID**:
- Stored as session cookie (expires when browser closes)
- 30-minute inactivity timeout
- Cross-site session stitching via `trace_session` URL parameter

**User ID**:
- Persists for 1 year in cookies
- Used for long-term user identification across sessions

### 5. Updated API Methods

**TRACE.identify(userId, options)**:
```javascript
// Set custom user ID (default 1 year expiration)
TRACE.identify('user-123');

// Set with custom expiration (30 days)
TRACE.identify('user-123', { expires: 30 });
```

**TRACE.reset()**:
- Now handles both cookie and localStorage removal
- Creates new session ID after reset

## Usage Examples

### Basic Cookie Usage

```html
<!-- Use cookies explicitly -->
<script src="https://trace.example.com/trace.js"
  data-collector="https://trace.example.com/collect"
  data-storage="cookie"></script>
```

### Cross-Domain Cookies

```html
<!-- Set cookie for all subdomains -->
<script src="https://trace.example.com/trace.js"
  data-collector="https://trace.example.com/collect"
  data-storage="cookie"
  data-cookie-domain=".example.com"></script>
```

### Secure Cookies (HTTPS)

```html
<!-- Secure cookies for HTTPS sites -->
<script src="https://trace.example.com/trace.js"
  data-collector="https://trace.example.com/collect"
  data-storage="cookie"
  data-cookie-secure="true"
  data-cookie-samesite="Strict"></script>
```

### Auto-Detection (Recommended)

```html
<!-- Automatically detect best storage method -->
<script src="https://trace.example.com/trace.js"
  data-collector="https://trace.example.com/collect"
  data-storage="auto"></script>
```

## Cookie Storage Details

| Cookie Name | Duration | Purpose |
|-------------|----------|---------|
| `trace_session_id` | Session | Current session identifier |
| `trace_session_start` | Session | Session start timestamp (for timeout) |
| `trace_user_id` | 1 year | Persistent user identifier |

## Benefits Over localStorage

1. **ITP Resistance**: Survives Safari's Intelligent Tracking Prevention
2. **Cross-Tab**: Works across tabs in private browsing
3. **Server Access**: Cookies sent with HTTP requests (future use)
4. **Wider Compatibility**: Works when localStorage is disabled

## Files Changed

- `client/trace.js`: Added cookie utilities and storage detection
- `client/trace.min.js`: Rebuilt minified version
- `client/package.json`: No changes (existing terser dependency)
- `notes/bf-qvk.md`: This documentation

## Testing

Manual testing performed:
1. Cookie creation and persistence across page refreshes
2. localStorage fallback when cookies are disabled
3. Session timeout handling
4. Cross-site session stitching with cookies

## Retrospective

- **What worked**: The dual-storage approach with auto-detection provides maximum compatibility. The cookie utility functions are simple and don't require external dependencies.

- **What didn't**: No significant issues. The implementation followed the existing pattern of the codebase.

- **Surprise**: Safari's ITP can completely block localStorage in some scenarios, making cookies essential for reliable tracking.

- **Reusable pattern**: For client-side identity persistence, always provide both cookie and localStorage options with auto-detection. The pattern of: 1) Detect availability, 2) Use best method, 3) Provide manual override.

## Status

COMPLETE - First-party cookie identity and persistence implemented.
