import fs from 'fs';
import path from 'path';
import type {
	ICredentialTestFunctions,
	ICredentialsDecrypted,
	INodeCredentialTestResult,
} from 'n8n-workflow';
import type { SqliteNodeCredentials } from '../helpers/interfaces';

export async function sqliteConnectionTest(
	this: ICredentialTestFunctions,
	credential: ICredentialsDecrypted,
): Promise<INodeCredentialTestResult> {
	const credentials = credential.data as SqliteNodeCredentials;
	const dbPath = credentials?.databasePath;

	if (!dbPath) {
		return { status: 'Error', message: 'Database path is missing' };
	}

	if (!path.isAbsolute(dbPath)) {
		return { status: 'Error', message: 'Database path must be absolute (e.g. /home/user/mydb.sqlite)' };
	}

	const dir = path.dirname(dbPath);

	try {
		fs.accessSync(dir, fs.constants.W_OK);
	} catch {
		return { status: 'Error', message: `Directory "${dir}" does not exist or is not writable` };
	}

	if (fs.existsSync(dbPath)) {
		try {
			fs.accessSync(dbPath, fs.constants.R_OK | fs.constants.W_OK);
		} catch {
			return { status: 'Error', message: `File "${dbPath}" exists but is not readable/writable` };
		}
	}

	return { status: 'OK', message: 'Path is valid and writable' };
}
