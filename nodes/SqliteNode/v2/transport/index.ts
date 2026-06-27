import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const nativeBinding = path.join(__dirname, '../../../../../native/node-v127-linux-musl-x64/better_sqlite3.node');
import type {
	ICredentialTestFunctions,
	IExecuteFunctions,
	ILoadOptionsFunctions,
} from 'n8n-workflow';
import type { SqliteDatabase, SqliteNodeCredentials } from '../helpers/interfaces';

export function createConnection(
	this: IExecuteFunctions | ILoadOptionsFunctions | ICredentialTestFunctions,
	credentials: SqliteNodeCredentials,
): SqliteDatabase {
	const dbPath = credentials.databasePath;
	if (!dbPath || typeof dbPath !== 'string') {
		throw new Error('Database file path is required');
	}
	const dir = path.dirname(dbPath);
	if (dir && dir !== '.') {
		try {
			if (!fs.existsSync(dir)) {
				fs.mkdirSync(dir, { recursive: true });
			}
		} catch (err) {
			const code = (err as NodeJS.ErrnoException)?.code;
			if (code === 'EACCES' || code === 'EPERM') {
				throw new Error(
					`Permission denied: cannot create directory "${dir}". Use a path in a location you can write to (e.g. your home or project folder).`,
				);
			}
			throw err;
		}
	}
	if (fs.existsSync(nativeBinding)) {
		try {
			return new Database(dbPath, { nativeBinding });
		} catch {}
	}
	return new Database(dbPath);
}

export function closeConnection(db: SqliteDatabase): void {
	db.close();
}
