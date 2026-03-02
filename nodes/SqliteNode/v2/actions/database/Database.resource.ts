import type { INodeProperties } from 'n8n-workflow';
import * as deleteOp from './delete.operation';
import * as executeQuery from './executeQuery.operation';
import * as insert from './insert.operation';
import * as select from './select.operation';
import * as update from './update.operation';
import * as upsert from './upsert.operation';
import { tableRLC } from '../common.descriptions';

export { deleteOp, executeQuery, insert, select, update, upsert };

export const description: INodeProperties[] = [
	{
		displayName: 'Operation',
		name: 'operation',
		type: 'options',
		noDataExpression: true,
		options: [
			{
				name: 'Delete',
				value: 'delete',
				description: 'Delete rows or drop a table',
				action: 'Delete rows or table',
			},
			{
				name: 'Execute SQL',
				value: 'executeQuery',
				description: 'Execute a raw SQL query',
				action: 'Execute a SQL query',
			},
			{
				name: 'Insert',
				value: 'insert',
				description: 'Insert rows into a table',
				action: 'Insert rows',
			},
			{
				name: 'Insert or Update',
				value: 'upsert',
				description: 'Insert or update rows (ON CONFLICT)',
				action: 'Insert or update rows',
			},
			{
				name: 'Select',
				value: 'select',
				description: 'Select rows from a table',
				action: 'Select rows',
			},
			{
				name: 'Update',
				value: 'update',
				description: 'Update rows in a table',
				action: 'Update rows',
			},
		],
		displayOptions: { show: { resource: ['database'] } },
		default: 'select',
	},
	{
		...tableRLC,
		displayOptions: { hide: { operation: ['executeQuery'] } },
	},
	...deleteOp.description,
	...executeQuery.description,
	...insert.description,
	...select.description,
	...update.description,
	...upsert.description,
];
