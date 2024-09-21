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
		icon: 'file:sqlite-icon.svg',
		group: ['transform'],
		version: 1,
		description: 'A node to perform query in a local sqlite database',
		defaults: {
			name: 'Sqlite Node',
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
				default: 'AUTO',
				noDataExpression: true,
				required: true,
				options: [
					{
						name: 'AUTO',
						value: 'AUTO',
						description: 'Automatically detect the query type',
					},
					{
						name: 'CREATE',
						value: 'CREATE',
						description: 'Create a table',
					},
					{
						name: 'DELETE',
						value: 'DELETE',
						description: 'Delete rows from a table',
					},
					{
						name: 'INSERT',
						value: 'INSERT',
						description: 'Insert rows into a table',
					},
					{
						name: 'SELECT',
						value: 'SELECT',
						description: 'Select rows from a table (support for multiple queries)',
					},
					{
						name: 'UPDATE',
						value: 'UPDATE',
						description: 'Update rows in a table',
					},
				],
			},
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				default: '',
				placeholder: 'SELECT * FROM table where key = $key',
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
				placeholder: '{"$key": "value"}',
				description: 'The args that get passed to the query',
			},
			{
				displayName: 'Spread Result',
				name: 'spread',
				type: 'boolean',
				default: false,
				description: 'Whether the result should be spread into multiple items',
				displayOptions: {
					show: {
						query_type: [
							'SELECT',
						],
					},
				},				
			}
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> 
	{
		const items = this.getInputData();
		let item: INodeExecutionData;

		let spreadResults = [];
		for(let itemIndex = 0; itemIndex < items.length; itemIndex++) 
		{
			try 
			{
				item = items[itemIndex];
				let db_path = this.getNodeParameter('db_path', itemIndex, '') as string;
				let query = this.getNodeParameter('query', itemIndex, '') as string;
				let args_string = this.getNodeParameter('args', itemIndex, '') as string;
				let args = JSON.parse(args_string);
				let query_type = this.getNodeParameter('query_type', itemIndex, '') as string;
				let spread = this.getNodeParameter('spread', itemIndex, '') as boolean;

				if(query_type === 'AUTO') 
				{
					if(query.trim().toUpperCase().includes('SELECT')) 
						query_type = 'SELECT';
					else if(query.trim().toUpperCase().includes('INSERT')) 
						query_type = 'INSERT';
					else if(query.trim().toUpperCase().includes('UPDATE')) 
						query_type = 'UPDATE';
					else if(query.trim().toUpperCase().includes('DELETE')) 
						query_type = 'DELETE';
					else if(query.trim().toUpperCase().includes('CREATE')) 
						query_type = 'CREATE';
					else 
						query_type = 'AUTO';
				}

				if(db_path === '') 
					throw new NodeOperationError(this.getNode(), 'No database path provided.');
				

				if(query === '') 
					throw new NodeOperationError(this.getNode(), 'No query provided.');

				const db = new sqlite3.Database(db_path);
				const results = await new Promise<any|any[]>(async (resolve, reject) => 
				{
					if(query_type === 'SELECT') 
					{
						// if query contains multiple queries, split them and execute them one by one
						let queries = query.split(';').filter(q => q.trim() !== '');
						if(queries.length > 1)
						{

							let results = await Promise.all(queries.map(async (q) => 
							{
								const query_args = { ...args };
								for(const key in query_args) 
								{
									if(!q.includes(key)) 
										delete query_args[key];
								}
									
								return await new Promise<any|any[]>(async (resolve1, reject1) => 
								{
									// For SELECT queries, use db.all() to get all rows
									db.all(q, query_args, (error, rows) => 
									{
										if(error) 
											return reject1(error);

										return resolve1(rows);
									});
								});
							}));

							return resolve(results);
						}

						const query_args = { ...args };
						for(const key in query_args) 
						{
							if(!query.includes(key)) 
								delete query_args[key];
						}

						// For SELECT queries, use db.all() to get all rows
						db.all(query, query_args, (error, rows) => 
						{
							if(error) 
								return reject(error);

							return resolve(rows);
						});
					} 
					else if(['INSERT', 'UPDATE', 'DELETE'].includes(query_type)) 
					{
						const query_args = { ...args };
						for(const key in query_args) 
						{
							if(!query.includes(key)) 
								delete query_args[key];
						}

						// For INSERT, UPDATE, DELETE queries, use db.run() 
						db.run(query, query_args, function (error) 
						{
							if(error) 
								return reject(error);
							// Provide information like affected rows, last inserted id, etc.
							return resolve({
								changes: this.changes, // Number of rows affected
								last_id: this.lastID // The last inserted row ID
							});
						});
					} 
					else 
					{
						const query_args = { ...args };
						for(const key in query_args) 
						{
							if(!query.includes(key)) 
								delete query_args[key];
						}

						// For other SQL commands (like CREATE, DROP, etc.), use db.run()
						db.run(query, query_args, (error) => 
						{
							if(error) 
								return reject(error);
							return resolve({ message: 'Query executed successfully.' });
						});
					}
				});
				db.close();

				if(query_type === 'SELECT' && spread) 
				{
					// If spread is true, spread the result into multiple items
					const newItems = results.map((result: any) => 
					{
						if(Array.isArray(result))
							return { json: {items: result} }; 
						else 
							return { json: result };
					});
					
					spreadResults.push(...newItems);
				} 
				else 
					item.json = results;
			} 
			catch(error) 
			{
				// This node should never fail but we want to showcase how
				// to handle errors.
				if(this.continueOnFail()) 
				{
					items.push({ json: this.getInputData(itemIndex)[0].json, error, pairedItem: itemIndex });
				} 
				else 
				{
					// Adding `itemIndex` allows other workflows to handle this error
					if(error.context) 
					{
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

		if(spreadResults.length > 0) 
			return this.prepareOutputData(spreadResults);

		return this.prepareOutputData(items);
	}
}