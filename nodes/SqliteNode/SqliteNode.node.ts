import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';
import * as sqlite3 from 'sqlite3';

export class SqliteNode implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'SQLite Node',
		name: 'SqliteNode',
		group: ['transform'],
		version: 1,
		description: 'A node to perform query in a local sqlite database',
		defaults: {
			name: 'SqliteNode',
		},
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			// Node properties which the user gets displayed and
			// can change on the node.
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
				default: 'SELECT',
				noDataExpression: true,
				required: true,
				options: [
					{
						name: 'CREATE',
						value: 'CREATE',
					},
					{
						name: 'DELETE',
						value: 'DELETE',
					},
					{
						name: 'INSERT',
						value: 'INSERT',
					},
					{
						name: 'SELECT',
						value: 'SELECT',
					},
					{
						name: 'UPDATE',
						value: 'UPDATE',
					},
				],
			},
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				default: '',
				placeholder: 'SELECT * FROM table',
				description: 'The query to execute',
				required: true,
				typeOptions: {
					rows: 8,
				},
			},
			{
				displayName: 'Args',
				name: 'args',
				type: 'json',
				default: '{}',
				placeholder: '{"key": "value"}',
				description: 'The args that get passed to the query',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();

		let item: INodeExecutionData;
		let db_path: string;
		let query: string;
		let args_string: string;
		let query_type: string;
		let args: any;

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				db_path = this.getNodeParameter('db_path', itemIndex, '') as string;
				query = this.getNodeParameter('query', itemIndex, '') as string;
				args_string = this.getNodeParameter('args', itemIndex, '') as string;
				query_type = this.getNodeParameter('query_type', itemIndex, '') as string;
				args = JSON.parse(args_string);

				item = items[itemIndex];

				const db = new sqlite3.Database(db_path);
				const results = await new Promise((resolve, reject) => {
					if (query_type === 'SELECT') {
						// For SELECT queries, use db.all() to get all rows
						db.all(query, args, (error, rows) => {
							if(error) 
								return reject(error);
							return resolve(rows);
						});
					} else if (['INSERT', 'UPDATE', 'DELETE'].includes(query_type)) {
						// For INSERT, UPDATE, DELETE queries, use db.run() 
						db.run(query, args, function (error) {
							if(error) 
								return reject(error);
							// Provide information like affected rows, last inserted id, etc.
							return resolve({
								changes: this.changes, // Number of rows affected
								last_id: this.lastID // The last inserted row ID
							});
						});
					} else {
						// For other SQL commands (like CREATE, DROP, etc.), use db.run()
						db.run(query, args, (error) => {
							if(error) 
								return reject(error);
							return resolve({ message: 'Query executed successfully.' });
						});
					}
				});
				db.close();

				item.json = results as any;
			} catch (error) {
				// This node should never fail but we want to showcase how
				// to handle errors.
				if (this.continueOnFail()) {
					items.push({ json: this.getInputData(itemIndex)[0].json, error, pairedItem: itemIndex });
				} else {
					// Adding `itemIndex` allows other workflows to handle this error
					if (error.context) {
						// If the error thrown already contains the context property,
						// only append the itemIndex
						error.context.itemIndex = itemIndex;
						throw error;
					}
					throw new NodeOperationError(this.getNode(), error, {
						itemIndex,
					});
				}
			}
		}

		return this.prepareOutputData(items);
	}
}