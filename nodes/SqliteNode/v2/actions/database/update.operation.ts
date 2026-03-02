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
		default: DATA_MODE.AUTO_MAP,
		description: 'How to map input data to table columns',
	},
	{
		displayName:
			'In this mode, ensure incoming field names match table column names.',
		name: 'notice',
		type: 'notice',
		default: '',
		displayOptions: { show: { dataMode: [DATA_MODE.AUTO_MAP] } },
	},
	{
		displayName: 'Column to Match On',
		name: 'columnToMatchOn',
		type: 'options',
		required: true,
		description: 'Column used to find the row to update (e.g. id)',
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
		description: 'Rows with this value in the match column will be updated',
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
						displayName: 'Column',
						name: 'column',
						type: 'options',
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
	show: { resource: ['database'], operation: ['update'] },
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
		let valueToMatchOn: string | number = '';

		if (dataMode === DATA_MODE.AUTO_MAP) {
			item = { ...items[i].json };
			valueToMatchOn = item[columnToMatchOn] as string | number;
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
			valueToMatchOn = this.getNodeParameter('valueToMatchOn', i) as string;
		}

		const updateColumns = Object.keys(item).filter((col) => col !== columnToMatchOn);
		if (updateColumns.length === 0) continue;

		const updates = updateColumns.map((col) => `${escapeSqlIdentifier(col)} = ?`);
		const values: QueryValues = [
			...updateColumns.map((col) => item[col] as string | number | null),
			valueToMatchOn,
		];
		const query = `UPDATE ${escapeSqlIdentifier(table)} SET ${updates.join(', ')} WHERE ${escapeSqlIdentifier(columnToMatchOn)} = ?`;
		queries.push({ query, values });
	}

	return runQueries(queries);
}
