import type { IDataObject } from 'n8n-workflow';
import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';
import type { Database as BetterSqlite3Database } from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';

const binaryPath = path.join(__dirname, '../../../../native/node-v127-linux-musl-x64/better_sqlite3.node');

async function all(db: BetterSqlite3Database, query: string, args: Record<string, unknown>): Promise<unknown> {
	return new Promise((resolve, reject) => {
		try {
			const rows = db.prepare(query).all(args);
			resolve(rows);
		} catch (error) {
			reject(error);
		}
	});
}

async function run(db: BetterSqlite3Database, query: string, args: Record<string, unknown>): Promise<unknown> {
	return new Promise((resolve, reject) => {
		try {
			const result = db.prepare(query).run(args);
			resolve({
				changes: result.changes,
				last_id: result.lastInsertRowid,
			});
		} catch (error) {
			reject(error);
		}
	});
}

async function exec(db: BetterSqlite3Database, query: string): Promise<unknown> {
	return new Promise((resolve, reject) => {
		try {
			db.exec(query);
			resolve({ message: 'Query executed successfully.' });
		} catch (error) {
			reject(error);
		}
	});
}

export class SqliteV1 implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'SQLite Node',
		name: 'sqliteNode',
		icon: 'file:../../assets/sqlite-icon.svg',
		group: ['transform'],
		version: 1,
		description: 'A node to perform query in a local sqlite database',
		defaults: {
			name: 'Sqlite Node',
		},
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			{
				displayName: 'Database Path',
				name: 'db_path',
				type: 'string',
				default: '',
				placeholder: '/path/to/database.sqlite',
				description: 'The path to the SQLite database file',
				required: true,
			},
			{
				displayName: 'Query Type',
				name: 'query_type',
				type: 'options',
				default: 'AUTO',
				noDataExpression: true,
				required: true,
				options: [
					{ name: 'AUTO', value: 'AUTO', description: 'Automatically detect the query type' },
					{ name: 'CREATE', value: 'CREATE', description: 'Create a table' },
					{ name: 'DELETE', value: 'DELETE', description: 'Delete rows from a table' },
					{ name: 'INSERT', value: 'INSERT', description: 'Insert rows into a table' },
					{ name: 'SELECT', value: 'SELECT', description: 'Select rows from a table' },
					{ name: 'UPDATE', value: 'UPDATE', description: 'Update rows in a table' },
				],
			},
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				default: '',
				placeholder: 'SELECT * FROM table where key = @key',
				description: 'The query to execute',
				required: true,
				typeOptions: { rows: 8 },
			},
			{
				displayName: 'Args',
				name: 'args',
				type: 'json',
				default: '{}',
				placeholder: '{"key": "value"}',
				description: 'The args that get passed to the query',
			},
			{
				displayName: 'Spread Result',
				name: 'spread',
				type: 'boolean',
				default: false,
				description: 'Whether the result should be spread into multiple items',
				displayOptions: { show: { query_type: ['SELECT'] } },
			},
			{
				displayName: 'Additional Options',
				name: 'additionalOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Use Default Bindings',
						name: 'use_default_bindings',
						type: 'boolean',
						default: false,
						description:
							'Whether you are running this outside of docker image and you want to use the default bindings for better-sqlite3',
					},
					{
						displayName: 'Use Custom Bindings',
						name: 'use_custom_bindings',
						type: 'string',
						default: binaryPath,
						description: 'Whether you want to provide your own better-sqlite3 bindings',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const outputItems: INodeExecutionData[] = [];

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			let dbPath = this.getNodeParameter('db_path', itemIndex, '') as string;
			let query = this.getNodeParameter('query', itemIndex, '') as string;
			const argsString = this.getNodeParameter('args', itemIndex, '') as string;
			let queryType = this.getNodeParameter('query_type', itemIndex, '') as string;
			const spread = this.getNodeParameter('spread', itemIndex, '') as boolean;

			const additionalOptions = this.getNodeParameter('additionalOptions', 0, {}) as {
				use_default_bindings?: boolean;
				use_custom_bindings?: string;
			};

			const useDefaultBindings = additionalOptions.use_default_bindings ?? false;
			const useCustomBindings = additionalOptions.use_custom_bindings;

			if (queryType === 'AUTO') {
				const q = query.trim().toUpperCase();
				if (q.includes('SELECT')) queryType = 'SELECT';
				else if (q.includes('INSERT')) queryType = 'INSERT';
				else if (q.includes('UPDATE')) queryType = 'UPDATE';
				else if (q.includes('DELETE')) queryType = 'DELETE';
				else if (q.includes('CREATE')) queryType = 'CREATE';
				else queryType = 'AUTO';
			}

			if (dbPath === '') throw new NodeOperationError(this.getNode(), 'No database path provided.');
			if (query === '') throw new NodeOperationError(this.getNode(), 'No query provided.');

			query = query.replace(/\$/g, '@');

			let bindings: Database.Options = { nativeBinding: binaryPath };
			if (useDefaultBindings) {
				bindings = {};
			}
			if (useCustomBindings) {
				if (fs.existsSync(useCustomBindings)) {
					bindings.nativeBinding = useCustomBindings;
				} else {
					throw new NodeOperationError(
						this.getNode(),
						`Custom bindings file not found at ${useCustomBindings}`,
					);
				}
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
						throw new NodeOperationError(
							this.getNode(),
							`Permission denied: cannot create directory "${dir}". Use a path in a location you can write to (e.g. your home or project folder).`,
						);
					}
					throw err;
				}
			}
			const db = new Database(dbPath, bindings);
			try {
				const argsT = JSON.parse(argsString || '{}') as Record<string, unknown>;
				const args: Record<string, unknown> = {};
				for (const key in argsT) {
					args[key.replace(/\$/g, '')] = argsT[key];
				}

				let results: unknown;
				if (queryType === 'SELECT') {
					const queries = query.split(';').filter((q) => q.trim() !== '');
					if (queries.length > 1) {
						results = await Promise.all(
							queries.map(async (q) => {
								const queryArgs = { ...args };
								for (const key in queryArgs) {
									if (!q.includes(key)) delete queryArgs[key];
								}
								return all(db, q, queryArgs);
							}),
						);
					} else {
						const queryArgs = { ...args };
						for (const key in queryArgs) {
							if (!query.includes(key)) delete queryArgs[key];
						}
						results = await all(db, query, queryArgs);
					}
				} else if (['INSERT', 'UPDATE', 'DELETE'].includes(queryType)) {
					const queryArgs = { ...args };
					for (const key in queryArgs) {
						if (!query.includes(key)) delete queryArgs[key];
					}
					results = await run(db, query, queryArgs);
				} else {
					const queryArgs = { ...args };
					for (const key in queryArgs) {
						if (!query.includes(key)) delete queryArgs[key];
					}
					results = await exec(db, query);
				}

				if (queryType === 'SELECT' && spread) {
					const resultArray = Array.isArray(results) ? results : [results];
					for (const result of resultArray) {
						const rows = Array.isArray(result) ? result : [result];
						for (const row of rows) {
						outputItems.push({
							json: row as IDataObject,
							pairedItem: { item: itemIndex },
						});
						}
					}
				} else {
				outputItems.push({
					json: results as IDataObject,
					pairedItem: { item: itemIndex },
				});
				}
			} catch (error) {
				if (this.continueOnFail()) {
					outputItems.push({
						json: {
							error: (error as Error).message || 'Unknown error',
						},
						pairedItem: { item: itemIndex },
					});
				} else {
					const err = error as Error & { context?: unknown };
					if (err.context) {
						(err.context as Record<string, unknown>).itemIndex = itemIndex;
						throw error;
					}
					throw new NodeOperationError(this.getNode(), error as Error, {
						itemIndex,
						message: (error as Error).message,
					});
				}
			} finally {
				db.close();
			}
		}

		return [outputItems];
	}
}
