import type { IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import * as database from './database/Database.resource';
import type { QueryRunner, QueryWithValues, SqliteNodeCredentials } from '../helpers/interfaces';
import { BATCH_MODE } from '../helpers/interfaces';
import { parseSqliteError } from '../helpers/utils';
import { closeConnection, createConnection } from '../transport';

export async function router(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
	const items = this.getInputData();
	const resource = this.getNodeParameter('resource', 0) as string;
	const operation = this.getNodeParameter('operation', 0) as string;
	const nodeOptions = (this.getNodeParameter('options', 0) as IDataObject) || {};
	const credentials = await this.getCredentials<SqliteNodeCredentials>('sqliteCredentials');

	const db = createConnection.call(this, credentials);

	const runQueries: QueryRunner = (queries: QueryWithValues[]): Promise<INodeExecutionData[]> => {
		const returnData: INodeExecutionData[] = [];
		const mode = (nodeOptions.queryBatching as string) || BATCH_MODE.SINGLE;

		const runOne = (q: QueryWithValues): void => {
			const isSelect = q.query.trim().toUpperCase().startsWith('SELECT');
			const stmt = db.prepare(q.query);
			if (isSelect) {
				const rows = stmt.all(...q.values) as IDataObject[];
				for (const row of rows) {
					returnData.push({ json: row });
				}
			} else {
				const result = stmt.run(...q.values) as { changes: number; lastInsertRowid: bigint };
				returnData.push({
					json: {
						changes: result.changes,
						lastInsertRowid: result.lastInsertRowid != null ? Number(result.lastInsertRowid) : undefined,
					},
				});
			}
		};

		try {
			if (mode === BATCH_MODE.TRANSACTION) {
				db.exec('BEGIN');
				try {
					for (const q of queries) {
						runOne(q);
					}
					db.exec('COMMIT');
				} catch (err) {
					db.exec('ROLLBACK');
					throw err;
				}
			} else {
				for (const q of queries) {
					runOne(q);
				}
			}
		} catch (err) {
			if (!this.continueOnFail()) {
				throw parseSqliteError(this.getNode(), err);
			}
			returnData.push({
				json: { error: (err as Error).message },
			});
		}

		return Promise.resolve(returnData);
	};

	try {
		let returnData: INodeExecutionData[] = [];

		if (resource === 'database') {
			// Map UI operation value to export key ('delete' is reserved, so exported as deleteOp)
			const opKey = (operation === 'delete' ? 'deleteOp' : operation) as keyof typeof database;
			if (typeof database[opKey] !== 'object' || !('execute' in database[opKey])) {
				throw new NodeOperationError(
					this.getNode(),
					`The operation "${operation}" is not supported!`,
				);
			}
			const execFn = (database[opKey] as { execute: (...args: unknown[]) => Promise<INodeExecutionData[]> }).execute;
			returnData = await execFn.call(this, items, runQueries, nodeOptions);
		} else {
			throw new NodeOperationError(this.getNode(), `The resource "${resource}" is not supported!`);
		}

		return [returnData];
	} finally {
		closeConnection(db);
	}
}
