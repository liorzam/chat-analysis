#!/usr/bin/env node
import { spawn, spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');
const mainJs = join(root, 'dist', 'main.js');

const req = createRequire(import.meta.url);
let tscJs;
try {
  tscJs = req.resolve('typescript/lib/tsc.js');
} catch {
  console.error('Missing dev dependency: typescript. Run pnpm install in the package root.');
  process.exit(1);
}

const compile = spawnSync(process.execPath, [tscJs], { cwd: root, stdio: 'inherit' });
if (compile.status !== 0) process.exit(compile.status ?? 1);

const child = spawn(process.execPath, [mainJs, ...process.argv.slice(2)], {
  stdio: 'inherit',
  cwd: root,
});

child.on('exit', (code, signal) => {
  if (signal) process.kill(process.pid, signal);
  process.exit(code ?? 1);
});
