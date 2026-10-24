// Rebuild the checked HTML from authored JSON. Run from any directory.
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const cli = process.argv[2];
if (!cli) {
  console.error('Usage: node docs/architecture/rebuild.mjs /path/to/archify/bin/archify.mjs');
  process.exit(2);
}
const absoluteCLI = resolve(cli);
const cwd = dirname(fileURLToPath(import.meta.url));
const repository = resolve(cwd, '../..');
const diagrams = JSON.parse(readFileSync(resolve(cwd, 'manifest.json'), 'utf8'));
const run = `review-${new Date().toISOString().replace(/[:.]/g, '-')}`;
for (const diagram of diagrams) {
  console.log(`Finalizing ${diagram.slug}`);
  const result = spawnSync(process.execPath, [
    absoluteCLI, 'finalize', diagram.type, diagram.candidate, diagram.html,
    '--repo-root', repository, '--quality', 'showcase',
    '--out-dir', `${dirname(diagram.html)}/${run}`, '--json',
  ], { cwd, stdio: 'inherit' });
  if (result.error) console.error(result.error.message);
  if (result.status !== 0) process.exit(result.status || 1);
}
console.log('HTML rebuilt. Review and refresh previews/verification.json before publishing.');
