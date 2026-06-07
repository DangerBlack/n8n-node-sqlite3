import type { IExecuteFunctions, INodeExecutionData, INodeProperties } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import type { QueryRunner, QueryValues, QueryWithValues } from '../../helpers/interfaces';
import { addWhereClauses, escapeSqlIdentifier, getWhereClauses, updateDisplayOptions } from '../../helpers/utils';
import {
	optionsCollection,
	selectRowsFixedCollection,
	combineConditionsCollection,
} from '../common.descriptions';

const properties: INodeProperties[] = [
	{
		displayName: 'Command',
		name: 'deleteCommand',
		type: 'options',
		default: 'delete',
		options: [
			{
				name: 'Delete Rows',
				value: 'delete',
				description: 'Delete rows matching conditions, or all rows if no conditions',
			},
			{
				name: 'Truncate',
				value: 'truncate',
				description: 'Delete all rows and keep the table structure',
			},
			{
				name: 'Drop Table',
				value: 'drop',
				description: 'Drop the table entirely',
			},
		],
	},
	{
		...selectRowsFixedCollection,
		displayOptions: { show: { deleteCommand: ['delete'] } },
	},
	{
		...combineConditionsCollection,
		displayOptions: { show: { deleteCommand: ['delete'] } },
	},
	optionsCollection,
];

const displayOptions = {
	show: { resource: ['database'], operation: ['delete'] },
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
		const deleteCommand = this.getNodeParameter('deleteCommand', i) as string;

		let query: string;
		let values: QueryValues = [];

		if (deleteCommand === 'drop') {
			query = `DROP TABLE IF EXISTS ${escapeSqlIdentifier(table)}`;
		} else if (deleteCommand === 'truncate') {
			query = `DELETE FROM ${escapeSqlIdentifier(table)}`;
		} else if (deleteCommand === 'delete') {
			const whereClauses = getWhereClauses(this, i);
			const combineConditions = this.getNodeParameter('combineConditions', i, 'AND') as string;
			[query, values] = addWhereClauses(
				this.getNode(),
				i,
				`DELETE FROM ${escapeSqlIdentifier(table)}`,
				whereClauses,
				values,
				combineConditions,
			);
		} else {
			throw new NodeOperationError(
				this.getNode(),
				'Invalid delete command. Use delete, truncate, or drop.',
				{ itemIndex: i },
			);
		}

		queries.push({ query, values });
	}

	return runQueries(queries);
}
