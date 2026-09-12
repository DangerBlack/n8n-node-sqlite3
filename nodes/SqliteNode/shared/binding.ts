import Database from 'better-sqlite3';
import type { Database as BetterSqlite3Database } from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

/**
 * Loading the SQLite native binding.
 *
 * Since v13 better-sqlite3 is an N-API addon and ships a prebuilt binary for every
 * supported platform inside its own npm tarball, so a plain `npm install` already has
 * a working binding — no compiler, no download, and no rebuild when n8n bumps its Node
 * version (N-API binaries are not tied to the Node ABI the way the old V8/NAN ones were).
 *
 * That is the path we take first. The fallbacks below exist for installations where the
 * shipped binary cannot be used: a locked-down image, an unusual platform, or a binding
 * the user compiled themselves.
 *
 *   1. `N8N_SQLITE3_NATIVE_BINDING`, if set, wins over everything.
 *   2. better-sqlite3's own prebuilt binary (the normal case, in Docker and natively).
 *   3. A binding bundled with this package under `native/` (see `npm run prebuilds`).
 *
 * better-sqlite3's own binary is preferred over our bundled copy because it is always
 * version-matched to the JavaScript that loads it.
 */

/** Point this at your own `better_sqlite3.node` to override the resolution below. */
export const NATIVE_BINDING_ENV_VAR = 'N8N_SQLITE3_NATIVE_BINDING';

const BINDING_FILE = 'better_sqlite3.node';
const NATIVE_DIR_NAME = 'native';
const MAX_ROOT_LOOKUP_DEPTH = 8;

export interface OpenDatabaseOptions {
	/** Explicit path to a binding, e.g. one the user built themselves. */
	nativeBinding?: string;
	/** Skip the bundled fallback and use better-sqlite3's own binary only. */
	useDefaultBindings?: boolean;
}

/**
 * musl and glibc builds are not interchangeable, and `process.platform` cannot tell
 * them apart. Node's diagnostic report exposes the runtime glibc version, which musl
 * systems (the n8n Alpine image) simply do not have.
 */
export function detectLibc(): 'musl' | 'glibc' | undefined {
	if (process.platform !== 'linux') return undefined;

	try {
		const report = process.report?.getReport() as
			| { header?: { glibcVersionRuntime?: string } }
			| undefined;
		if (report?.header) {
			return report.header.glibcVersionRuntime ? 'glibc' : 'musl';
		}
	} catch {}

	return fs.existsSync('/etc/alpine-release') ? 'musl' : 'glibc';
}

/** Directory name of the bundled binding for this runtime, e.g. `linux-musl-x64`. */
export function platformTag(): string {
	const libc = detectLibc();
	const parts = [process.platform as string];
	if (libc) parts.push(libc);
	parts.push(process.arch);
	return parts.join('-');
}

/** Walk up from the compiled file until we find the package's `native/` directory. */
function findNativeDir(): string | undefined {
	let dir = __dirname;

	for (let depth = 0; depth <= MAX_ROOT_LOOKUP_DEPTH; depth++) {
		const candidate = path.join(dir, NATIVE_DIR_NAME);
		if (fs.existsSync(candidate)) return candidate;

		const parent = path.dirname(dir);
		if (parent === dir) break;
		dir = parent;
	}

	return undefined;
}

/** Bindings bundled with this package, for diagnostics. */
export function bundledTargets(): string[] {
	const nativeDir = findNativeDir();
	if (!nativeDir) return [];

	try {
		return fs
			.readdirSync(nativeDir)
			.filter((entry) => fs.existsSync(path.join(nativeDir, entry, BINDING_FILE)))
			.sort();
	} catch {
		return [];
	}
}

/**
 * Path of the bundled binding matching this runtime, or `undefined` when none is
 * bundled for it. Pre-v13 layouts (`node-v<abi>-<platform>-<libc>-<arch>`) are still
 * accepted so bindings baked by hand against an older release keep working.
 */
export function resolveNativeBinding(): string | undefined {
	const nativeDir = findNativeDir();
	if (!nativeDir) return undefined;

	const candidates = [
		platformTag(),
		`node-v${process.versions.modules}-${platformTag()}`,
	];

	for (const candidate of candidates) {
		const binding = path.join(nativeDir, candidate, BINDING_FILE);
		if (fs.existsSync(binding)) return binding;
	}

	return undefined;
}

function bindingOverride(): string | undefined {
	const override = process.env[NATIVE_BINDING_ENV_VAR];
	if (!override) return undefined;

	if (!fs.existsSync(override)) {
		throw new Error(`${NATIVE_BINDING_ENV_VAR} points at "${override}", which does not exist.`);
	}

	return override;
}

function bindingFailureMessage(reason: string): string {
	const bundled = bundledTargets();

	return [
		`Could not load the SQLite native binding: ${reason}`,
		`Runtime: ${platformTag()}, Node ${process.version}.`,
		`Bundled bindings: ${bundled.length ? bundled.join(', ') : 'none'}.`,
		`Set ${NATIVE_BINDING_ENV_VAR} to a "${BINDING_FILE}" built for this platform to override.`,
	].join(' ');
}

/**
 * Open a database, trying each binding source in turn so the same package works inside
 * the n8n Docker image and on a plain Node installation.
 */
export function openDatabase(
	dbPath: string,
	options: OpenDatabaseOptions = {},
): BetterSqlite3Database {
	const candidates: Array<string | undefined> = [];

	const explicit = options.nativeBinding ?? bindingOverride();
	if (explicit) {
		candidates.push(explicit);
	} else {
		// better-sqlite3's own prebuilt binary, version-matched to the library.
		candidates.push(undefined);
		if (!options.useDefaultBindings) {
			const bundled = resolveNativeBinding();
			if (bundled) candidates.push(bundled);
		}
	}

	let lastError: unknown;
	for (const nativeBinding of candidates) {
		try {
			return nativeBinding ? new Database(dbPath, { nativeBinding }) : new Database(dbPath);
		} catch (error) {
			lastError = error;
		}
	}

	const reason = (lastError as Error)?.message ?? 'unknown error';
	throw new Error(bindingFailureMessage(reason));
}
