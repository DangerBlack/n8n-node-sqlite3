import type { IExecuteFunctions, INodeType, INodeTypeDescription } from 'n8n-workflow';
import type { INodeTypeBaseDescription } from 'n8n-workflow';
import { router } from './actions/router';
import { searchTables } from './methods/listSearch';
import { getColumns, getColumnsMultiOptions } from './methods/loadOptions';
import { versionDescription } from './actions/versionDescription';

export class SqliteV2 implements INodeType {
	description: INodeTypeDescription;

	constructor(baseDescription: INodeTypeBaseDescription) {
		this.description = {
			...baseDescription,
			...versionDescription,
		};
	}

	methods = {
		listSearch: { searchTables },
		loadOptions: { getColumns, getColumnsMultiOptions },
	};

	async execute(this: IExecuteFunctions) {
		return await router.call(this);
	}
}
