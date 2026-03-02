import type { ILoadOptionsFunctions, INodeListSearchResult } from 'n8n-workflow';
import type { SqliteNodeCredentials } from '../helpers/interfaces';
import { closeConnection, createConnection } from '../transport';

export async function searchTables(this: ILoadOptionsFunctions): Promise<INodeListSearchResult> {
	const credentials = await this.getCredentials<SqliteNodeCredentials>('sqliteCredentials');
	const db = createConnection.call(this, credentials);
	try {
		const rows = db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all() as { name: string }[];
		const results = rows.map((row) => ({
			name: row.name,
			value: row.name,
		}));
		return { results };
	} finally {
		closeConnection(db);
	}
}
