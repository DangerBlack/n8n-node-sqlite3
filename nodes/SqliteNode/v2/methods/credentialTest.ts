import type {
	ICredentialTestFunctions,
	ICredentialsDecrypted,
	INodeCredentialTestResult,
} from 'n8n-workflow';
import type { SqliteNodeCredentials } from '../helpers/interfaces';
import { closeConnection, createConnection } from '../transport';

export async function sqliteConnectionTest(
	this: ICredentialTestFunctions,
	credential: ICredentialsDecrypted,
): Promise<INodeCredentialTestResult> {
	const credentials = credential.data as SqliteNodeCredentials;
	if (!credentials?.databasePath) {
		return { status: 'Error', message: 'Database path is missing' };
	}
	try {
		const db = createConnection.call(this, credentials);
		closeConnection(db);
		return { status: 'OK', message: 'Connection successful!' };
	} catch (error) {
		return {
			status: 'Error',
			message: (error as Error).message ?? 'Connection failed',
		};
	}
}
