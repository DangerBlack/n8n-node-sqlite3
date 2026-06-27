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
		displayName: 'Allow Expressions in Query (Unsafe)',
		name: 'allowExpressions',
		type: 'boolean',
		default: false,
		noDataExpression: true,
		description: 'Whether to allow n8n expressions inside the SQL query. When enabled, expressions like {{ $JSON.value }} are evaluated before the query is sent to SQLite. Only enable this with trusted data, passing unsanitized user input into the query string can lead to SQL injection.',
	},
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
		displayOptions: { show: { allowExpressions: [false] } },
	},
	{
		displayName: 'Query',
		name: 'queryExpression',
		type: 'string',
		default: '',
		placeholder: 'e.g. SELECT * FROM {{ $json.tableName }}',
		required: true,
		description:
			"The SQL query to execute. Expressions are evaluated before the query runs, do not pass unsanitized user input here.",
		typeOptions: { rows: 5 },
		displayOptions: { show: { allowExpressions: [true] } },
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
		const allowExpressions = this.getNodeParameter('allowExpressions', i, false) as boolean;
		const rawQuery = allowExpressions
			? (this.getNodeParameter('queryExpression', i) as string)
			: (this.getNodeParameter('query', i) as string);
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
