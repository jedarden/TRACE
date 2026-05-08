#!/usr/bin/env bash

# TRACE Client Build Script
# Builds minified production bundle using available tools
#
# Usage:
#   ./build.sh           # Auto-detect best available method
#   ./build.sh npm       # Use npm/terser (requires npm install)
#   ./build.sh online    # Use online minification service
#   ./build.sh copy      # Just copy without minification

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

INPUT="trace.js"
OUTPUT="trace.min.js"

echo "Building TRACE client bundle..."

# Method 1: npm/terser (best quality, requires npm)
build_npm() {
    if command -v npm &> /dev/null; then
        if [ ! -d "node_modules" ]; then
            echo "Installing dependencies..."
            npm install
        fi
        echo "Building with npm/terser..."
        node build.js
        return 0
    else
        echo "npm not found, trying next method..."
        return 1
    fi
}

# Method 2: Online minification service (toptal, etc.)
build_online() {
    echo "Building using online minification service..."

    # Use toptal's online minifier
    response=$(curl -s -X POST \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-urlencode "input=$(< "$INPUT")" \
        "https://www.toptal.com/developers/javascript-minifier/api/raw" 2>/dev/null || echo "")

    if [ -n "$response" ] && [ ${#response} -gt 100 ]; then
        # Add source map comment and banner
        cat > "$OUTPUT" << 'EOF'
/*!
 * TRACE - Traffic Recording, Attribution, and Campaign Events
 * Client-side tracking library
 * @version 1.0.0
 * @license MIT
 */
EOF
        echo "$response" >> "$OUTPUT"
        echo "//# sourceMappingURL=trace.min.js.map" >> "$OUTPUT"
        echo "  ✓ Created $OUTPUT (${#response} bytes)"
        echo "  (Source map not available with online method)"
        return 0
    else
        echo "Online minification failed, trying next method..."
        return 1
    fi
}

# Method 3: Simple copy (no minification)
build_copy() {
    echo "Copying without minification..."
    cp "$INPUT" "$OUTPUT"
    local size=$(wc -c < "$OUTPUT")
    echo "  ✓ Created $OUTPUT ($size bytes)"
    echo "  (No minification applied)"
    return 0
}

# Try methods in order
METHOD="${1:-auto}"

case "$METHOD" in
    npm)
        build_npm || exit 1
        ;;
    online)
        build_online || build_copy
        ;;
    copy)
        build_copy
        ;;
    auto)
        build_npm || build_online || build_copy
        ;;
    *)
        echo "Unknown method: $METHOD"
        echo "Usage: $0 [npm|online|copy]"
        exit 1
        ;;
esac

echo ""
echo "Build complete!"
echo "  Input:  $INPUT"
echo "  Output: $OUTPUT"
