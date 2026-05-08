#!/usr/bin/env node

/**
 * TRACE Client Build Script
 * Minifies trace.js and generates source map
 */

const { minify } = require('terser');
const fs = require('fs');
const path = require('path');

async function build() {
  console.log('Building TRACE client bundle...');

  const inputFile = path.join(__dirname, 'trace.js');
  const outputFile = path.join(__dirname, 'trace.min.js');
  const mapFile = path.join(__dirname, 'trace.min.js.map');

  // Read source
  const source = fs.readFileSync(inputFile, 'utf8');

  try {
    // Minify with terser
    const result = await minify(source, {
      compress: {
        dead_code: true,
        drop_console: false,
        drop_debugger: true,
        conditionals: true,
        evaluate: true,
        side_effects: true
      },
      mangle: {
        toplevel: false,
        properties: false
      },
      format: {
        comments: /^!/,
        ascii_only: false
      },
      sourceMap: {
        filename: 'trace.min.js',
        url: 'trace.min.js.map'
      },
      ecma: 5,
      keep_classnames: false,
      keep_fnames: false
    });

    if (result.error) {
      console.error('Minification error:', result.error);
      process.exit(1);
    }

    // Write minified output
    fs.writeFileSync(outputFile, result.code);
    console.log(`  ✓ Created ${outputFile} (${result.code.length} bytes)`);

    // Write source map
    if (result.map) {
      fs.writeFileSync(mapFile, result.map);
      console.log(`  ✓ Created ${mapFile}`);
    }

    // Calculate size reduction
    const originalSize = source.length;
    const minifiedSize = result.code.length;
    const reduction = ((1 - minifiedSize / originalSize) * 100).toFixed(1);

    console.log(`\n  Original: ${originalSize} bytes`);
    console.log(`  Minified: ${minifiedSize} bytes (${reduction}% reduction)`);
    console.log('\nBuild complete!');

  } catch (err) {
    console.error('Build failed:', err);
    process.exit(1);
  }
}

build();
