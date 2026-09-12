#!/usr/bin/env node
/**
 * Copies better-sqlite3's prebuilt bindings out of node_modules into `native/`, so the
 * package can ship a binding of its own.
 *
 * This is a safety net, not the main path: since v13 better-sqlite3 is an N-API addon
 * and already ships a binary for every platform inside its npm tarball, which is what
 * `nodes/SqliteNode/shared/binding.ts` loads first. The bundled copy only matters for
 * installations where that binary cannot be used — and because N-API binaries are not
 * tied to the Node ABI, it no longer has to be redone every time n8n bumps Node.
 *
 * Usage:
 *   npm run prebuilds                        # musl x64 + arm64 (the n8n Docker image)
 *   npm run prebuilds -- --all               # every platform better-sqlite3 ships
 *   npm run prebuilds -- --targets=linux-x64,darwin-arm64
 *   npm run prebuilds -- --clean             # drop bundled bindings not selected
 *   npm run prebuilds -- --dry-run
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const NATIVE_DIR = path.join(ROOT, 'native');
const PREBUILDS_DIR = path.join(ROOT, 'node_modules', 'better-sqlite3', 'prebuilds');
const BINDING_FILE = 'better_sqlite3.node';
const VERSION_STAMP = '.better-sqlite3-version';

/** n8n's official image is Alpine, so musl is what actually needs shipping. */
const DEFAULT_TARGETS = ['linuxmusl-x64', 'linuxmusl-arm64'];

/** Upstream names its prebuilds `linuxmusl-x64`; we store them as `linux-musl-x64`. */
function targetDirName(target) {
	if (target.startsWith('linuxmusl-')) return `linux-musl-${target.slice('linuxmusl-'.length)}`;
	if (target.startsWith('linux-')) return `linux-glibc-${target.slice('linux-'.length)}`;
	return target;
}

function parseArgs(argv) {
	const args = { all: false, clean: false, dryRun: false, targets: undefined };

	for (const arg of argv) {
		if (arg === '--all') args.all = true;
		else if (arg === '--clean') args.clean = true;
		else if (arg === '--dry-run') args.dryRun = true;
		else if (arg.startsWith('--targets=')) args.targets = arg.slice('--targets='.length).split(',');
		else {
			console.error(`Unknown argument: ${arg}`);
			process.exit(1);
		}
	}

	return args;
}

function availableTargets() {
	if (!fs.existsSync(PREBUILDS_DIR)) {
		throw new Error(
			`No prebuilt bindings at ${path.relative(ROOT, PREBUILDS_DIR)}. ` +
				'Run "npm install" first — better-sqlite3 v13 or newer is required.',
		);
	}

	return fs
		.readdirSync(PREBUILDS_DIR)
		.filter((entry) => entry.endsWith('.node'))
		.map((entry) => entry.slice(0, -'.node'.length))
		.sort();
}

function formatSize(bytes) {
	return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function main() {
	const args = parseArgs(process.argv.slice(2));
	const available = availableTargets();
	const version = JSON.parse(
		fs.readFileSync(path.join(ROOT, 'node_modules', 'better-sqlite3', 'package.json'), 'utf8'),
	).version;

	const requested = args.all ? available : (args.targets ?? DEFAULT_TARGETS);
	const missing = requested.filter((target) => !available.includes(target));
	if (missing.length) {
		throw new Error(
			`better-sqlite3 v${version} ships no prebuild for: ${missing.join(', ')}. ` +
				`Available: ${available.join(', ')}.`,
		);
	}

	console.log(`better-sqlite3 v${version}`);

	const keep = new Set(requested.map(targetDirName));
	let total = 0;

	for (const target of requested) {
		const dirName = targetDirName(target);
		const destination = path.join(NATIVE_DIR, dirName);
		const source = path.join(PREBUILDS_DIR, `${target}.node`);
		const size = fs.statSync(source).size;
		total += size;

		if (args.dryRun) {
			console.log(`  would copy ${target} -> native/${dirName}`);
			continue;
		}

		fs.mkdirSync(destination, { recursive: true });
		fs.copyFileSync(source, path.join(destination, BINDING_FILE));
		fs.writeFileSync(path.join(destination, VERSION_STAMP), `${version}\n`);
		console.log(`  ${dirName.padEnd(24)} ${formatSize(size).padStart(8)}`);
	}

	if (args.clean && fs.existsSync(NATIVE_DIR)) {
		for (const entry of fs.readdirSync(NATIVE_DIR)) {
			if (keep.has(entry)) continue;
			console.log(`  removing stale ${entry}`);
			if (!args.dryRun) fs.rmSync(path.join(NATIVE_DIR, entry), { recursive: true, force: true });
		}
	}

	if (!args.dryRun) {
		console.log(`\n${requested.length} bindings, ${formatSize(total)} total.`);
	}
}

try {
	main();
} catch (error) {
	console.error(`\n${error.message}`);
	process.exit(1);
}
