import type { IDataObject } from 'n8n-workflow';
import {
	IExecuteFunctions,
	INode,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';
import type { Database as BetterSqlite3Database } from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';

enum QueryType {
	AUTO = 'AUTO',
	CREATE = 'CREATE',
	DELETE = 'DELETE',
	INSERT = 'INSERT',
	SELECT = 'SELECT',
	UPDATE = 'UPDATE',
}

interface AdditionalOptions {
	use_default_bindings?: boolean;
	use_custom_bindings?: string;
}

const binaryPath = path.join(__dirname, '../../../../native/node-v127-linux-musl-x64/better_sqlite3.node');

function detectQueryType(query: string): QueryType {
	const upper = query.trim().toUpperCase();
	if (/^SELECT\b/.test(upper)) return QueryType.SELECT;
	if (/^INSERT\b/.test(upper)) return QueryType.INSERT;
	if (/^UPDATE\b/.test(upper)) return QueryType.UPDATE;
	if (/^DELETE\b/.test(upper)) return QueryType.DELETE;
	if (/^CREATE\b/.test(upper)) return QueryType.CREATE;
	return QueryType.AUTO;
}

function parseArgs(node: INode, argsString: string): Record<string, unknown> {
	try {
		const raw = JSON.parse(argsString || '{}') as Record<string, unknown>;
		const args: Record<string, unknown> = {};
		for (const key in raw) {
			args[key.replace(/\$/g, '')] = raw[key];
		}
		return args;
	} catch {
		throw new NodeOperationError(node, 'Args must be valid JSON.');
	}
}

function filterArgs(query: string, args: Record<string, unknown>): Record<string, unknown> {
	const used: Record<string, unknown> = {};
	for (const key in args) {
		if (query.includes(key)) used[key] = args[key];
	}
	return used;
}

function getBindings(node: INode, opts: AdditionalOptions): Database.Options {
	if (opts.use_default_bindings) return {};
	if (opts.use_custom_bindings) {
		if (!fs.existsSync(opts.use_custom_bindings)) {
			throw new NodeOperationError(node, `Custom bindings file not found at ${opts.use_custom_bindings}`);
		}
		return { nativeBinding: opts.use_custom_bindings };
	}
	return { nativeBinding: binaryPath };
}

function wrapError(node: INode, error: unknown, itemIndex: number): never {
	const err = error as Error & { context?: Record<string, unknown> };
	if (err.context) {
		err.context.itemIndex = itemIndex;
		throw error;
	}
	throw new NodeOperationError(node, err, { itemIndex, message: err.message });
}

function all(db: BetterSqlite3Database, query: string, args: Record<string, unknown>): unknown[] {
	return db.prepare(query).all(args);
}

function run(db: BetterSqlite3Database, query: string, args: Record<string, unknown>): { changes: number; last_id: number | bigint } {
	const result = db.prepare(query).run(args);
	return { changes: result.changes, last_id: result.lastInsertRowid };
}

function exec(db: BetterSqlite3Database, query: string): { message: string } {
	db.exec(query);
	return { message: 'Query executed successfully.' };
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
					{ name: 'SELECT', value: 'SELECT', description: 'Select rows from a table (support for multiple queries)' },
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
			const dbPath = this.getNodeParameter('db_path', itemIndex, '') as string;
			let query = this.getNodeParameter('query', itemIndex, '') as string;
			const argsString = this.getNodeParameter('args', itemIndex, '{}') as string;
			let queryType = this.getNodeParameter('query_type', itemIndex, QueryType.AUTO) as QueryType;
			const spread = this.getNodeParameter('spread', itemIndex, false) as boolean;
			const additionalOptions = this.getNodeParameter('additionalOptions', itemIndex, {}) as AdditionalOptions;

			if (!dbPath) throw new NodeOperationError(this.getNode(), 'No database path provided.');
			if (!query) throw new NodeOperationError(this.getNode(), 'No query provided.');

			query = query.replace(/\$/g, '@');
			queryType = queryType === QueryType.AUTO ? detectQueryType(query) : queryType;

			const bindings = getBindings(this.getNode(), additionalOptions);

			const dir = path.dirname(dbPath);
			if (dir && dir !== '.') {
				try {
					if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
				} catch (err) {
					const code = (err as NodeJS.ErrnoException)?.code;
					if (code === 'EACCES' || code === 'EPERM') {
						throw new NodeOperationError(
							this.getNode(),
							`Permission denied: cannot create directory "${dir}". Use a path in a location you can write to.`,
						);
					}
					throw err;
				}
			}

			const db = new Database(dbPath, bindings);
			try {
				const args = parseArgs(this.getNode(), argsString);
				let results: unknown;

				if (queryType === QueryType.SELECT) {
					const queries = query.split(';').filter((q) => q.trim() !== '');
					results = queries.length > 1
						? await Promise.all(queries.map((q) => all(db, q, filterArgs(q, args))))
						: all(db, query, filterArgs(query, args));
				} else if ([QueryType.INSERT, QueryType.UPDATE, QueryType.DELETE].includes(queryType)) {
					results = run(db, query, filterArgs(query, args));
				} else {
					results = exec(db, query);
				}

				if (queryType === QueryType.SELECT && spread) {
					const resultArray = Array.isArray(results) ? results : [results];
					for (const result of resultArray) {
						const rows = Array.isArray(result) ? result : [result];
						for (const row of rows) {
							outputItems.push({ json: row as IDataObject, pairedItem: { item: itemIndex } });
						}
					}
				} else {
					outputItems.push({ json: results as IDataObject, pairedItem: { item: itemIndex } });
				}
			} catch (error) {
				if (this.continueOnFail()) {
					outputItems.push({
						json: { error: (error as Error).message || 'Unknown error' },
						pairedItem: { item: itemIndex },
					});
				} else {
					wrapError(this.getNode(), error, itemIndex);
				}
			} finally {
				db.close();
			}
		}

		return [outputItems];
	}
}
