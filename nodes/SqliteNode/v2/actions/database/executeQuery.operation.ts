import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeProperties,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import type { QueryRunner, QueryValues, QueryWithValues } from '../../helpers/interfaces';
import {
	prepareQueryAndReplacements,
	replaceEmptyStringsByNulls,
	updateDisplayOptions,
} from '../../helpers/utils';
import { optionsCollection } from '../common.descriptions';

const properties: INodeProperties[] = [
	{
		displayName: 'Query',
		name: 'query',
		type: 'string',
		default: '',
		placeholder: 'e.g. SELECT id, name FROM product WHERE id < 40',
		required: true,
		description:
			"The SQL query to execute. Use $1, $2, $3, etc. for parameters and set values in Options 'Query Parameters'.",
		noDataExpression: true,
		typeOptions: { rows: 5 },
	},
	optionsCollection,
];

const displayOptions = {
	show: { resource: ['database'], operation: ['executeQuery'] },
};

export const description = updateDisplayOptions(displayOptions, properties);

export async function execute(
	this: IExecuteFunctions,
	inputItems: INodeExecutionData[],
	runQueries: QueryRunner,
	nodeOptions: IDataObject,
): Promise<INodeExecutionData[]> {
	const items = replaceEmptyStringsByNulls(inputItems, nodeOptions.replaceEmptyStrings as boolean);
	const queries: QueryWithValues[] = [];

	for (let i = 0; i < items.length; i++) {
		const rawQuery = this.getNodeParameter('query', i) as string;
		const options = this.getNodeParameter('options', i, {}) as IDataObject;
		let queryReplacement = options.queryReplacement;

		if (queryReplacement === undefined || queryReplacement === null) {
			queryReplacement = [];
		} else if (typeof queryReplacement === 'string') {
			queryReplacement = (queryReplacement as string).split(',').map((s: string) => s.trim());
		}

		if (!Array.isArray(queryReplacement)) {
			throw new NodeOperationError(
				this.getNode(),
				'Query Parameters must be a comma-separated string or array',
				{ itemIndex: i },
			);
		}

		const values = queryReplacement as QueryValues;
		const { query, values: preparedValues } = prepareQueryAndReplacements(rawQuery, values);
		queries.push({ query, values: preparedValues });
	}

	return runQueries(queries);
}
