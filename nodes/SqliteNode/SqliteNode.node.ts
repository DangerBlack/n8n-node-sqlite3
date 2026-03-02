import type { INodeTypeBaseDescription, IVersionedNodeType } from 'n8n-workflow';
import { VersionedNodeType } from 'n8n-workflow';
import { SqliteV1 } from './v1/SqliteV1.node';
import { SqliteV2 } from './v2/SqliteV2.node';

export class SqliteNode extends VersionedNodeType {
	constructor() {
		const baseDescription: INodeTypeBaseDescription = {
			displayName: 'SQLite',
			name: 'sqliteNode',
			icon: 'file:../../assets/sqlite-icon.svg',
			group: ['transform'],
			defaultVersion: 2,
			description: 'Get, add, update, and delete data in a SQLite database',
			parameterPane: 'wide',
		};

		const nodeVersions: IVersionedNodeType['nodeVersions'] = {
			1: new SqliteV1(),
			2: new SqliteV2(baseDescription),
		};

		super(nodeVersions, baseDescription);
	}
}
