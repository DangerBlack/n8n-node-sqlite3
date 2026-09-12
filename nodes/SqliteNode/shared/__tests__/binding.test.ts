import fs from 'fs';
import os from 'os';
import path from 'path';
import {
	NATIVE_BINDING_ENV_VAR,
	bindingCandidates,
	bundledTargets,
	detectLibc,
	openDatabase,
	platformTag,
	resolveNativeBinding,
} from '../binding';

describe('native binding resolution', () => {
	const originalOverride = process.env[NATIVE_BINDING_ENV_VAR];

	afterEach(() => {
		if (originalOverride === undefined) delete process.env[NATIVE_BINDING_ENV_VAR];
		else process.env[NATIVE_BINDING_ENV_VAR] = originalOverride;
	});

	it('should describe the running platform', () => {
		const tag = platformTag();
		expect(tag.startsWith(process.platform)).toBe(true);
		expect(tag.endsWith(process.arch)).toBe(true);
		if (process.platform === 'linux') {
			expect(tag).toContain(detectLibc() as string);
		}
	});

	it('should only report libc on linux', () => {
		const libc = detectLibc();
		if (process.platform === 'linux') expect(['musl', 'glibc']).toContain(libc);
		else expect(libc).toBeUndefined();
	});

	it('should list the bindings bundled with the package', () => {
		// The package ships musl builds for the n8n Docker image.
		expect(bundledTargets()).toEqual(expect.arrayContaining(['linux-musl-x64']));
	});

	it('should only resolve a bundled binding that exists for this runtime', () => {
		const resolved = resolveNativeBinding();
		if (resolved !== undefined) {
			expect(fs.existsSync(resolved)).toBe(true);
			expect(resolved).toContain(platformTag());
		}
	});

	it('should try better-sqlite3 own binding before the bundled fallback', () => {
		const candidates = bindingCandidates();
		expect(candidates[0]).toBeUndefined();
		expect(candidates.slice(1)).toEqual(
			resolveNativeBinding() === undefined ? [] : [resolveNativeBinding()],
		);
	});

	it('should only use better-sqlite3 own binding when the fallback is skipped', () => {
		expect(bindingCandidates({ useDefaultBindings: true })).toEqual([undefined]);
	});

	it('should use a binding chosen on the node instead of the bundled ones', () => {
		const chosen = '/opt/custom/better_sqlite3.node';
		expect(bindingCandidates({ nativeBinding: chosen })).toEqual([chosen]);
	});

	it('should let the environment override win over a binding chosen on the node', () => {
		process.env[NATIVE_BINDING_ENV_VAR] = __filename;
		expect(bindingCandidates({ nativeBinding: '/opt/custom/better_sqlite3.node' })).toEqual([
			__filename,
		]);
	});

	it('should reject an override pointing at a missing file', () => {
		process.env[NATIVE_BINDING_ENV_VAR] = path.join(os.tmpdir(), 'does-not-exist.node');
		expect(() => openDatabase(':memory:')).toThrow(NATIVE_BINDING_ENV_VAR);
	});

	it('should report database errors as themselves, not as binding failures', () => {
		const missingDir = path.join(os.tmpdir(), 'n8n-sqlite3-missing-dir', 'db.sqlite');
		expect(() => openDatabase(missingDir)).toThrow(/directory does not exist/);
		expect(() => openDatabase(missingDir)).not.toThrow(/native binding/);
	});

	it('should open a database with the binding better-sqlite3 ships', () => {
		const db = openDatabase(':memory:');
		try {
			db.exec('CREATE TABLE t (a INTEGER)');
			db.prepare('INSERT INTO t VALUES (@a)').run({ a: 1 });
			expect(db.prepare('SELECT a FROM t').all()).toEqual([{ a: 1 }]);
		} finally {
			db.close();
		}
	});

	it('should still open a database when the bundled fallback is skipped', () => {
		const db = openDatabase(':memory:', { useDefaultBindings: true });
		try {
			expect(db.prepare('SELECT 1 AS one').get()).toEqual({ one: 1 });
		} finally {
			db.close();
		}
	});
});
