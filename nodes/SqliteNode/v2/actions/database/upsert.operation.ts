import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeProperties,
} from 'n8n-workflow';
import type { QueryRunner, QueryValues, QueryWithValues } from '../../helpers/interfaces';
import { DATA_MODE } from '../../helpers/interfaces';
import { escapeSqlIdentifier, replaceEmptyStringsByNulls, updateDisplayOptions } from '../../helpers/utils';
import { optionsCollection } from '../common.descriptions';

const properties: INodeProperties[] = [
	{
		displayName: 'Data Mode',
		name: 'dataMode',
		type: 'options',
		default: 'autoMapInputData',
		options: [
			{
				name: 'Auto-Map Input Data to Columns',
				value: DATA_MODE.AUTO_MAP,
				description: 'Use when input field names match table column names',
			},
			{
				name: 'Map Each Column Manually',
				value: DATA_MODE.MANUAL,
				description: 'Set the value for each column manually',
			},
		],
		description: 'How to map input data to table columns',
	},
	{
		displayName: 'In this mode, ensure incoming field names match table column names.',
		name: 'notice',
		type: 'notice',
		default: '',
		displayOptions: { show: { dataMode: [DATA_MODE.AUTO_MAP] } },
	},
	{
		displayName: 'Column to Match On Name or ID',
		name: 'columnToMatchOn',
		type: 'options',
		required: true,
		description: 'Unique column used to detect conflict (e.g. ID). Must have a UNIQUE constraint. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
		typeOptions: {
			loadOptionsMethod: 'getColumns',
			loadOptionsDependsOn: ['table.value'],
		},
		default: '',
	},
	{
		displayName: 'Value of Column to Match On',
		name: 'valueToMatchOn',
		type: 'string',
		default: '',
		description: 'Value for the match column (manual mode only)',
		displayOptions: { show: { dataMode: [DATA_MODE.MANUAL] } },
	},
	{
		displayName: 'Values to Send',
		name: 'valuesToSend',
		placeholder: 'Add Value',
		type: 'fixedCollection',
		typeOptions: { multipleValueButtonText: 'Add Value', multipleValues: true },
		displayOptions: { show: { dataMode: [DATA_MODE.MANUAL] } },
		default: {},
		options: [
			{
				displayName: 'Values',
				name: 'values',
				values: [
					{
						displayName: 'Column Name or ID',
						name: 'column',
						type: 'options',
						description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
						typeOptions: {
							loadOptionsMethod: 'getColumns',
							loadOptionsDependsOn: ['table.value'],
						},
						default: '',
					},
					{ displayName: 'Value', name: 'value', type: 'string', default: '' },
				],
			},
		],
	},
	optionsCollection,
];

const displayOptions = {
	show: { resource: ['database'], operation: ['upsert'] },
	hide: { table: [''] },
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
		const table = this.getNodeParameter('table', i, '', { extractValue: true }) as string;
		const columnToMatchOn = this.getNodeParameter('columnToMatchOn', i) as string;
		const dataMode = this.getNodeParameter('dataMode', i) as string;

		let item: IDataObject = {};

		if (dataMode === DATA_MODE.AUTO_MAP) {
			item = { ...items[i].json };
		} else {
			const valuesToSend = (this.getNodeParameter('valuesToSend', i, { values: [] }) as IDataObject)
				.values as IDataObject[] | undefined;
			item = (valuesToSend ?? []).reduce(
				(acc, { column, value }: { column?: string; value?: string }) => {
					if (column) acc[column] = value ?? '';
					return acc;
				},
				{} as IDataObject,
			);
			item[columnToMatchOn] = this.getNodeParameter('valueToMatchOn', i) as string;
		}

		const columns = Object.keys(item);
		const escapedColumns = columns.map(escapeSqlIdentifier).join(', ');
		const placeholder = `(${columns.map(() => '?').join(',')})`;
		const updateColumns = columns.filter((c) => c !== columnToMatchOn);
		const updates = updateColumns.map((c) => `${escapeSqlIdentifier(c)} = excluded.${escapeSqlIdentifier(c)}`).join(', ');

		const query = `INSERT INTO ${escapeSqlIdentifier(table)} (${escapedColumns}) VALUES ${placeholder} ON CONFLICT(${escapeSqlIdentifier(columnToMatchOn)}) DO UPDATE SET ${updates}`;
		const values = Object.values(item) as QueryValues;
		queries.push({ query, values });
	}

	return runQueries(queries);
}
