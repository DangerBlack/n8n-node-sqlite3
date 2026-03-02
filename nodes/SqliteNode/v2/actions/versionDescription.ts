import type { INodeTypeDescription } from 'n8n-workflow';
import * as database from './database/Database.resource';

export const versionDescription: INodeTypeDescription = {
	displayName: 'SQLite',
	name: 'sqliteNode',
	icon: 'file:../../assets/sqlite-icon.svg',
	group: ['transform'],
	version: 2,
	subtitle: '={{ $parameter["operation"] }}',
	description: 'Get, add, update, and delete data in a SQLite database',
	defaults: {
		name: 'SQLite',
	},
	inputs: ['main'],
	outputs: ['main'],
	credentials: [
		{
			name: 'sqliteCredentials',
			required: true,
			testedBy: 'sqliteConnectionTest',
		},
	],
	properties: [
		{
			displayName: 'Resource',
			name: 'resource',
			type: 'hidden',
			noDataExpression: true,
			options: [{ name: 'Database', value: 'database' }],
			default: 'database',
		},
		...database.description,
	],
};
