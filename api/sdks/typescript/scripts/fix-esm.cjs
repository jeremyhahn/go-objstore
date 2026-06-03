// Post-build fixup for the ESM output.
//
// tsc with "module": "ES2020" does not rewrite relative import/export
// specifiers to include the ".js" extension that Node's native ESM loader
// requires. This script walks dist/esm and appends ".js" to every relative
// specifier that lacks an extension, then writes a package.json marking the
// folder as ESM. It also marks dist/cjs as CommonJS. The script is idempotent.
const fs = require('fs');
const path = require('path');

const distDir = path.join(__dirname, '..', 'dist');
const esmDir = path.join(distDir, 'esm');
const cjsDir = path.join(distDir, 'cjs');

// Matches: import ... from './x'   export ... from '../y'   import('./z')
const specifierRe =
  /((?:import|export)\s[^'"]*?from\s*|import\s*\(\s*)(['"])(\.\.?\/[^'"]+)\2/g;

function rewrite(code) {
  return code.replace(specifierRe, (match, head, quote, spec) => {
    // Leave specifiers that already carry a file extension untouched.
    if (/\.[a-zA-Z0-9]+$/.test(spec)) {
      return match;
    }
    return `${head}${quote}${spec}.js${quote}`;
  });
}

function walk(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walk(full);
    } else if (entry.name.endsWith('.js')) {
      const code = fs.readFileSync(full, 'utf8');
      const fixed = rewrite(code);
      if (fixed !== code) fs.writeFileSync(full, fixed);
    }
  }
}

if (!fs.existsSync(esmDir)) {
  console.error(`fix-esm: ${esmDir} not found; run tsc first`);
  process.exit(1);
}

walk(esmDir);
fs.writeFileSync(path.join(esmDir, 'package.json'), JSON.stringify({ type: 'module' }) + '\n');
fs.writeFileSync(path.join(cjsDir, 'package.json'), JSON.stringify({ type: 'commonjs' }) + '\n');
console.log('fix-esm: rewrote relative ESM specifiers; wrote dist/esm and dist/cjs package.json');
