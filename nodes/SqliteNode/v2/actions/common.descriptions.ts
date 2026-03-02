import type { INodeProperties, INodePropertyOptions } from 'n8n-workflow';
import { BATCH_MODE, SINGLE } from '../helpers/interfaces';

export const operatorOptions: INodePropertyOptions[] = [
	{ name: 'Equal', value: 'equal' },
	{ name: 'Not Equal', value: '!=' },
	{ name: 'Like', value: 'LIKE' },
	{ name: 'Greater Than', value: '>' },
	{ name: 'Less Than', value: '<' },
	{ name: 'Greater Than Or Equal', value: '>=' },
	{ name: 'Less Than Or Equal', value: '<=' },
	{ name: 'Is Null', value: 'IS NULL' },
	{ name: 'Is Not Null', value: 'IS NOT NULL' },
];

export const tableRLC: INodeProperties = {
	displayName: 'Table',
	name: 'table',
	type: 'resourceLocator',
	default: { mode: 'list', value: '' },
	required: true,
	description: 'The table to work on',
	modes: [
		{
			displayName: 'From List',
			name: 'list',
			type: 'list',
			placeholder: 'Select a Table...',
			typeOptions: {
				searchListMethod: 'searchTables',
				searchable: true,
			},
		},
		{
			displayName: 'Name',
			name: 'name',
			type: 'string',
			placeholder: 'table_name',
		},
	],
};

export const optionsCollection: INodeProperties = {
	displayName: 'Options',
	name: 'options',
	type: 'collection',
	default: {},
	placeholder: 'Add option',
	options: [
		{
			displayName: 'Query Batching',
			name: 'queryBatching',
			type: 'options',
			noDataExpression: true,
			description: 'How to run queries for multiple items',
			options: [
				{ name: 'Single Query', value: BATCH_MODE.SINGLE, description: 'One query for all items' },
				{
					name: 'Independent',
					value: BATCH_MODE.INDEPENDENTLY,
					description: 'One query per item',
				},
				{
					name: 'Transaction',
					value: BATCH_MODE.TRANSACTION,
					description: 'All queries in a transaction (rollback on failure)',
				},
			],
			default: SINGLE,
		},
		{
			displayName: 'Replace Empty Strings with NULL',
			name: 'replaceEmptyStrings',
			type: 'boolean',
			default: false,
			description: 'Whether to treat empty strings as NULL for insert/update',
			displayOptions: {
				show: { '/operation': ['insert', 'update', 'upsert', 'executeQuery'] },
			},
		},
		{
			displayName: 'Output Query Execution Details',
			name: 'detailedOutput',
			type: 'boolean',
			default: false,
			description: 'Include executed query details in output',
		},
		{
			displayName: 'Output Columns',
			name: 'outputColumns',
			type: 'multiOptions',
			typeOptions: {
				loadOptionsMethod: 'getColumnsMultiOptions',
				loadOptionsDependsOn: ['table.value'],
			},
			default: [],
			displayOptions: { show: { '/operation': ['select'] } },
		},
		{
			displayName: 'Select Distinct',
			name: 'selectDistinct',
			type: 'boolean',
			default: false,
			description: 'Return distinct rows only',
			displayOptions: { show: { '/operation': ['select'] } },
		},
		{
			displayName: 'Skip on Conflict',
			name: 'skipOnConflict',
			type: 'boolean',
			default: false,
			description: 'Use INSERT OR IGNORE to skip rows that violate unique constraints',
			displayOptions: { show: { '/operation': ['insert'] } },
		},
		{
			displayName: 'Query Parameters',
			name: 'queryReplacement',
			type: 'string',
			default: '',
			placeholder: 'e.g. value1,value2,value3',
			description: 'Comma-separated values to use as $1, $2, $3 in Execute SQL',
			displayOptions: { show: { '/operation': ['executeQuery'] } },
		},
	],
};

export const selectRowsFixedCollection: INodeProperties = {
	displayName: 'Select Rows',
	name: 'where',
	type: 'fixedCollection',
	typeOptions: { multipleValues: true },
	placeholder: 'Add Condition',
	default: {},
	description: 'Conditions to filter rows (leave empty for all rows)',
	options: [
		{
			displayName: 'Values',
			name: 'values',
			values: [
				{
					displayName: 'Column',
					name: 'column',
					type: 'options',
					description: 'Column to filter on',
					default: '',
					typeOptions: {
						loadOptionsMethod: 'getColumns',
						loadOptionsDependsOn: ['table.value'],
					},
				},
				{
					displayName: 'Operator',
					name: 'condition',
					type: 'options',
					description: 'Operator for the condition',
					options: operatorOptions,
					default: 'equal',
				},
				{
					displayName: 'Value',
					name: 'value',
					type: 'string',
					displayOptions: { hide: { condition: ['IS NULL', 'IS NOT NULL'] } },
					default: '',
				},
			],
		},
	],
};

export const sortFixedCollection: INodeProperties = {
	displayName: 'Sort',
	name: 'sort',
	type: 'fixedCollection',
	typeOptions: { multipleValues: true },
	placeholder: 'Add Sort Rule',
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
					default: '',
					typeOptions: {
						loadOptionsMethod: 'getColumns',
						loadOptionsDependsOn: ['table.value'],
					},
				},
				{
					displayName: 'Direction',
					name: 'direction',
					type: 'options',
					options: [
						{ name: 'ASC', value: 'ASC' },
						{ name: 'DESC', value: 'DESC' },
					],
					default: 'ASC',
				},
			],
		},
	],
};

export const combineConditionsCollection: INodeProperties = {
	displayName: 'Combine Conditions',
	name: 'combineConditions',
	type: 'options',
	description: 'How to combine WHERE conditions',
	options: [
		{ name: 'AND', value: 'AND', description: 'All conditions must match' },
		{ name: 'OR', value: 'OR', description: 'At least one condition must match' },
	],
	default: 'AND',
};
