import type {
	IDataObject,
	IExecuteFunctions,
	INode,
	INodeExecutionData,
	INodeProperties,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import type { QueryValues, SortRule, WhereClause } from './interfaces';
import type { SqliteDatabase } from './interfaces';

export function updateDisplayOptions(
	displayOptions: { show?: Record<string, unknown[]>; hide?: Record<string, unknown[]> },
	properties: INodeProperties[],
): INodeProperties[] {
	return properties.map((p) => ({
		...p,
		displayOptions: {
			show: { ...p.displayOptions?.show, ...displayOptions?.show },
			hide: { ...p.displayOptions?.hide, ...displayOptions?.hide },
		} as INodeProperties['displayOptions'],
	}));
}

export function escapeSqlIdentifier(identifier: string): string {
	const escaped = identifier.replace(/"/g, '""');
	return `"${escaped}"`;
}

export function replaceEmptyStringsByNulls(
	items: INodeExecutionData[],
	replace?: boolean,
): INodeExecutionData[] {
	if (!replace) return [...items];
	return items.map((item) => {
		const newItem = { ...item, json: { ...item.json } };
		for (const key of Object.keys(newItem.json)) {
			if (newItem.json[key] === '') {
				newItem.json[key] = null;
			}
		}
		return newItem;
	});
}

export function addWhereClauses(
	node: INode,
	itemIndex: number,
	query: string,
	clauses: WhereClause[],
	replacements: QueryValues,
	combineConditions?: string,
): [string, QueryValues] {
	if (clauses.length === 0) return [query, replacements];

	const combineWith = combineConditions === 'OR' ? 'OR' : 'AND';
	let whereQuery = ' WHERE';
	const values: QueryValues = [];

	clauses.forEach((clause, index) => {
		let cond = clause.condition === 'equal' ? '=' : clause.condition;
		let val = clause.value;
		if (['>', '<', '>=', '<='].includes(cond)) {
			const numVal = Number(clause.value);
			if (Number.isNaN(numVal)) {
				throw new NodeOperationError(
					node,
					`Operator in entry ${index + 1} of 'Select Rows' works with numbers, but value ${clause.value} is not a number`,
					{ itemIndex },
				);
			}
			val = numVal;
		}

		let valueReplacement = ' ';
		if (cond !== 'IS NULL' && cond !== 'IS NOT NULL') {
			valueReplacement = ' ?';
			values.push(val);
		}

		const operator = index === clauses.length - 1 ? '' : ` ${combineWith}`;
		whereQuery += ` ${escapeSqlIdentifier(clause.column)} ${cond}${valueReplacement}${operator}`;
	});

	return [`${query}${whereQuery}`, replacements.concat(values)];
}

export function addSortRules(
	query: string,
	rules: SortRule[],
	replacements: QueryValues,
): [string, QueryValues] {
	if (rules.length === 0) return [query, replacements];
	let orderByQuery = ' ORDER BY';
	rules.forEach((rule, index) => {
		const endWith = index === rules.length - 1 ? '' : ',';
		const direction = rule.direction === 'ASC' ? 'ASC' : 'DESC';
		orderByQuery += ` ${escapeSqlIdentifier(rule.column)} ${direction}${endWith}`;
	});
	return [`${query}${orderByQuery}`, replacements];
}

const CONDITION_SET = new Set([
	'equal', '!=', 'LIKE', '>', '<', '>=', '<=', 'IS NULL', 'IS NOT NULL', '=',
]);

export function isWhereClause(clause: unknown): clause is WhereClause {
	if (typeof clause !== 'object' || clause === null) return false;
	if (!('column' in clause) || typeof (clause as WhereClause).column !== 'string') return false;
	if (
		!('condition' in clause) ||
		typeof (clause as WhereClause).condition !== 'string' ||
		!CONDITION_SET.has((clause as WhereClause).condition)
	)
		return false;
	return true;
}

export function getWhereClauses(ctx: IExecuteFunctions, itemIndex: number): WhereClause[] {
	const whereClauses = ctx.getNodeParameter('where', itemIndex, []) as IDataObject;
	const values = whereClauses.values as unknown[];
	if (!Array.isArray(values)) return [];
	const invalid = values.some((c) => !isWhereClause(c));
	if (invalid) {
		throw new NodeOperationError(ctx.getNode(), 'Invalid where clause', { itemIndex });
	}
	return values as WhereClause[];
}

export function prepareQueryAndReplacements(
	rawQuery: string,
	replacements: QueryValues,
): { query: string; values: QueryValues } {
	const values: QueryValues = [];
	let query = rawQuery;
	const regex = /\$(\d+)/g;
	let match: RegExpExecArray | null;
	while ((match = regex.exec(rawQuery)) !== null) {
		const idx = Number(match[1]) - 1;
		if (idx >= 0 && idx < replacements.length) {
			values.push(replacements[idx]);
		}
	}
	query = rawQuery.replace(/\$\d+/g, '?');
	return { query, values };
}

export function parseSqliteError(
	node: INode,
	error: unknown,
	itemIndex?: number,
): NodeOperationError {
	const err = error as Error;
	return new NodeOperationError(node, err, { itemIndex, message: err.message });
}

export function runQueries(
	db: SqliteDatabase,
	queries: { query: string; values: QueryValues }[],
): INodeExecutionData[] {
	const returnData: INodeExecutionData[] = [];
	for (const { query, values } of queries) {
		const stmt = db.prepare(query);
		const result = stmt.run(...values) as { changes?: number; lastInsertRowid?: bigint };
		returnData.push({
			json: {
				changes: result.changes ?? 0,
				lastInsertRowid: result.lastInsertRowid != null ? Number(result.lastInsertRowid) : undefined,
			},
		});
	}
	return returnData;
}

export function runSelectQueries(
	db: SqliteDatabase,
	queries: { query: string; values: QueryValues }[],
): INodeExecutionData[] {
	const returnData: INodeExecutionData[] = [];
	for (const { query, values } of queries) {
		const stmt = db.prepare(query);
		const rows = stmt.all(...values) as IDataObject[];
		for (const row of rows) {
			returnData.push({ json: row });
		}
	}
	return returnData;
}
