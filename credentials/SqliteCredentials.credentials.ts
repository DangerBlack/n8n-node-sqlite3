import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class SqliteCredentials implements ICredentialType {
	name = 'sqliteCredentialsApi';

	displayName = 'SQLite Database';

	documentationUrl = 'sqlite';

	properties: INodeProperties[] = [
		{
			displayName: 'Database File Path',
			name: 'databasePath',
			type: 'string',
			default: '',
			placeholder: '/path/to/database.sqlite',
			description: 'Absolute path to the SQLite database file',
			required: true,
		},
	];
}
