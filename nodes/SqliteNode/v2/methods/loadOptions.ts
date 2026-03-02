import type { ILoadOptionsFunctions, INodePropertyOptions } from 'n8n-workflow';
import type { PragmaTableInfoRow, SqliteNodeCredentials } from '../helpers/interfaces';
import { escapeSqlIdentifier } from '../helpers/utils';
import { closeConnection, createConnection } from '../transport';

export async function getColumns(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
	const credentials = await this.getCredentials<SqliteNodeCredentials>('sqliteCredentials');
	const table = this.getNodeParameter('table', 0, { extractValue: true }) as string;
	if (!table) {
		return [];
	}
	const db = createConnection.call(this, credentials);
	try {
		const columns = db.prepare(`PRAGMA table_info(${escapeSqlIdentifier(table)})`).all() as PragmaTableInfoRow[];
		return columns.map((col) => ({
			name: col.name,
			value: col.name,
			description: `type: ${col.type || 'ANY'}, nullable: ${col.notnull === 0}`,
		}));
	} finally {
		closeConnection(db);
	}
}

export async function getColumnsMultiOptions(
	this: ILoadOptionsFunctions,
): Promise<INodePropertyOptions[]> {
	const columns = await getColumns.call(this);
	return [{ name: '*', value: '*', description: 'All columns' }, ...columns];
}
