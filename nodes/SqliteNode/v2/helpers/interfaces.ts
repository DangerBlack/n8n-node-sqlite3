import type Database from 'better-sqlite3';
import type { IDataObject, INodeExecutionData } from 'n8n-workflow';

export type SqliteDatabase = Database.Database;

export type SqliteNodeCredentials = {
	databasePath: string;
};

export type QueryValues = Array<string | number | IDataObject | null>;
export type QueryWithValues = { query: string; values: QueryValues };

export type QueryRunner = (queries: QueryWithValues[]) => Promise<INodeExecutionData[]>;

export type WhereClause = { column: string; condition: string; value: string | number };
export type SortRule = { column: string; direction: string };

export const AUTO_MAP = 'autoMapInputData';
const MANUAL = 'defineBelow';
export const DATA_MODE = {
	AUTO_MAP,
	MANUAL,
};

export const SINGLE = 'single';
const TRANSACTION = 'transaction';
const INDEPENDENTLY = 'independently';
export const BATCH_MODE = { SINGLE, TRANSACTION, INDEPENDENTLY };

export type QueryMode = typeof SINGLE | typeof TRANSACTION | typeof INDEPENDENTLY;

export type PragmaTableInfoRow = {
	cid: number;
	name: string;
	type: string;
	notnull: 0 | 1;
	dflt_value: string | null;
	pk: 0 | 1;
};
