import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeProperties,
} from 'n8n-workflow';
import type { QueryRunner, QueryValues, QueryWithValues, SortRule } from '../../helpers/interfaces';
import {
	addSortRules,
	addWhereClauses,
	escapeSqlIdentifier,
	getWhereClauses,
	updateDisplayOptions,
} from '../../helpers/utils';
import {
	optionsCollection,
	selectRowsFixedCollection,
	sortFixedCollection,
	combineConditionsCollection,
} from '../common.descriptions';

const properties: INodeProperties[] = [
	{
		displayName: 'Return All',
		name: 'returnAll',
		type: 'boolean',
		default: false,
		description: 'Whether to return all results or only up to a limit',
	},
	{
		displayName: 'Limit',
		name: 'limit',
		type: 'number',
		default: 50,
		description: 'Max number of results to return',
		typeOptions: { minValue: 1 },
		displayOptions: { show: { returnAll: [false] } },
	},
	selectRowsFixedCollection,
	combineConditionsCollection,
	sortFixedCollection,
	optionsCollection,
];

const displayOptions = {
	show: { resource: ['database'], operation: ['select'] },
	hide: { table: [''] },
};

export const description = updateDisplayOptions(displayOptions, properties);

export async function execute(
	this: IExecuteFunctions,
	inputItems: INodeExecutionData[],
	runQueries: QueryRunner,
): Promise<INodeExecutionData[]> {
	const queries: QueryWithValues[] = [];

	for (let i = 0; i < inputItems.length; i++) {
		const table = this.getNodeParameter('table', i, '', { extractValue: true }) as string;
		const outputColumns = (this.getNodeParameter('options.outputColumns', i, ['*']) as string[]) || ['*'];
		const selectDistinct = this.getNodeParameter('options.selectDistinct', i, false) as boolean;

		const SELECT = selectDistinct ? 'SELECT DISTINCT' : 'SELECT';
		let query: string;
		if (outputColumns.includes('*')) {
			query = `${SELECT} * FROM ${escapeSqlIdentifier(table)}`;
		} else {
			const escapedColumns = outputColumns.map(escapeSqlIdentifier).join(', ');
			query = `${SELECT} ${escapedColumns} FROM ${escapeSqlIdentifier(table)}`;
		}

		let values: QueryValues = [];
		const whereClauses = getWhereClauses(this, i);
		const combineConditions = this.getNodeParameter('combineConditions', i, 'AND') as string;
		[query, values] = addWhereClauses(
			this.getNode(),
			i,
			query,
			whereClauses,
			values,
			combineConditions,
		);

		const sortRules = ((this.getNodeParameter('sort', i, { values: [] }) as IDataObject).values as SortRule[]) || [];
		[query, values] = addSortRules(query, sortRules, values);

		const returnAll = this.getNodeParameter('returnAll', i, false) as boolean;
		if (!returnAll) {
			const limit = this.getNodeParameter('limit', i, 50) as number;
			query += ' LIMIT ?';
			values.push(limit);
		}

		queries.push({ query, values });
	}

	return runQueries(queries);
}
