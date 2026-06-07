import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeProperties,
} from 'n8n-workflow';
import type { QueryMode, QueryRunner, QueryValues, QueryWithValues } from '../../helpers/interfaces';
import { BATCH_MODE, DATA_MODE } from '../../helpers/interfaces';
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
		displayName:
			'In this mode, ensure incoming field names match table column names. Use an Edit Fields node if needed.',
		name: 'notice',
		type: 'notice',
		default: '',
		displayOptions: { show: { dataMode: [DATA_MODE.AUTO_MAP] } },
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
	show: { resource: ['database'], operation: ['insert'] },
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
	const table = this.getNodeParameter('table', 0, '', { extractValue: true }) as string;
	const dataMode = this.getNodeParameter('dataMode', 0) as string;
	const queryBatching = (nodeOptions.queryBatching as QueryMode) || BATCH_MODE.SINGLE;
	const ignore = (nodeOptions.skipOnConflict as boolean) ? 'OR IGNORE' : '';
	const queries: QueryWithValues[] = [];

	if (queryBatching === BATCH_MODE.SINGLE) {
		let columns: string[] = [];
		let insertItems: IDataObject[] = [];

		if (dataMode === DATA_MODE.AUTO_MAP) {
			columns = [
				...new Set(items.reduce((acc, item) => acc.concat(Object.keys(item.json)), [] as string[])),
			];
			insertItems = items.map((item) => {
				const row: IDataObject = {};
				for (const col of columns) {
					row[col] = item.json[col];
				}
				return row;
			});
		} else {
			for (let i = 0; i < items.length; i++) {
				const valuesToSend = (this.getNodeParameter('valuesToSend', i, { values: [] }) as IDataObject)
					.values as IDataObject[] | undefined;
				const item = (valuesToSend ?? []).reduce(
					(acc, { column, value }: { column?: string; value?: string }) => {
						if (column) acc[column] = value ?? '';
						return acc;
					},
					{} as IDataObject,
				);
				insertItems.push(item);
			}
			columns = [
				...new Set(insertItems.reduce((acc, item) => acc.concat(Object.keys(item)), [] as string[])),
			];
		}

		const escapedColumns = columns.map(escapeSqlIdentifier).join(', ');
		const placeholder = `(${columns.map(() => '?').join(',')})`;
		const replacements = insertItems.map(() => placeholder).join(',');
		const query = `INSERT ${ignore} INTO ${escapeSqlIdentifier(table)} (${escapedColumns}) VALUES ${replacements}`;
		const values: QueryValues = insertItems.reduce(
			(acc: QueryValues, item) => acc.concat(Object.values(item) as QueryValues),
			[],
		);
		queries.push({ query, values });
	} else {
		for (let i = 0; i < items.length; i++) {
			let columns: string[] = [];
			let insertItem: IDataObject = {};
			const options = this.getNodeParameter('options', i) as IDataObject;
			const ignoreItem = (options.skipOnConflict as boolean) ? 'OR IGNORE' : '';

			if (dataMode === DATA_MODE.AUTO_MAP) {
				columns = Object.keys(items[i].json);
				insertItem = { ...items[i].json };
			} else {
				const valuesToSend = (this.getNodeParameter('valuesToSend', i, { values: [] }) as IDataObject)
					.values as IDataObject[] | undefined;
				insertItem = (valuesToSend ?? []).reduce(
					(acc, { column, value }: { column?: string; value?: string }) => {
						if (column) acc[column] = value ?? '';
						return acc;
					},
					{} as IDataObject,
				);
				columns = Object.keys(insertItem);
			}

			const escapedColumns = columns.map(escapeSqlIdentifier).join(', ');
			const placeholder = `(${columns.map(() => '?').join(',')})`;
			const query = `INSERT ${ignoreItem} INTO ${escapeSqlIdentifier(table)} (${escapedColumns}) VALUES ${placeholder}`;
			const values = Object.values(insertItem) as QueryValues;
			queries.push({ query, values });
		}
	}

	return runQueries(queries);
}
