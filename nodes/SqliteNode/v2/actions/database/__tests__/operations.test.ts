import type { IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import * as select from '../select.operation';
import * as insert from '../insert.operation';
import * as update from '../update.operation';
import * as deleteOp from '../delete.operation';
import * as upsert from '../upsert.operation';
import * as executeQuery from '../executeQuery.operation';

function makeMock(params: Record<string, unknown>): IExecuteFunctions {
	return {
		getNodeParameter: (name: string, _i: number, defaultValue?: unknown) =>
			name in params ? params[name] : defaultValue,
		getNode: () => ({ name: 'SqliteNode', id: '1', type: 'sqliteNode', typeVersion: 2, position: [0, 0] }),
		continueOnFail: () => false,
	} as unknown as IExecuteFunctions;
}

const item = (json: IDataObject = {}): INodeExecutionData => ({ json });
const mockRunner = () => jest.fn().mockResolvedValue([{ json: { changes: 1 } }]);

// ─── select ──────────────────────────────────────────────────────────────────

describe('select.operation', () => {
	it('builds SELECT * with default LIMIT', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: false, limit: 50, where: { values: [] }, sort: { values: [] }, 'options.outputColumns': ['*'], 'options.selectDistinct': false, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT * FROM "employees" LIMIT ?', values: [50] }]);
	});

	it('builds SELECT with specific columns', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: false, limit: 10, where: { values: [] }, sort: { values: [] }, 'options.outputColumns': ['id', 'name'], 'options.selectDistinct': false, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT "id", "name" FROM "employees" LIMIT ?', values: [10] }]);
	});

	it('builds SELECT DISTINCT', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: false, limit: 50, where: { values: [] }, sort: { values: [] }, 'options.outputColumns': ['*'], 'options.selectDistinct': true, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT DISTINCT * FROM "employees" LIMIT ?', values: [50] }]);
	});

	it('builds SELECT with WHERE clause', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: false, limit: 50, where: { values: [{ column: 'id', condition: 'equal', value: 1 }] }, sort: { values: [] }, 'options.outputColumns': ['*'], 'options.selectDistinct': false, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT * FROM "employees" WHERE "id" = ? LIMIT ?', values: [1, 50] }]);
	});

	it('builds SELECT with multiple WHERE clauses combined with OR', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: false, limit: 50, where: { values: [{ column: 'id', condition: 'equal', value: 1 }, { column: 'name', condition: 'equal', value: 'Alice' }] }, sort: { values: [] }, 'options.outputColumns': ['*'], 'options.selectDistinct': false, combineConditions: 'OR' }),
			[item()],
			run,
		);
		const [call] = run.mock.calls[0][0];
		expect(call.query).toContain('OR');
		expect(call.values).toContain(1);
		expect(call.values).toContain('Alice');
	});

	it('builds SELECT with ORDER BY', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: false, limit: 50, where: { values: [] }, sort: { values: [{ column: 'name', direction: 'ASC' }] }, 'options.outputColumns': ['*'], 'options.selectDistinct': false, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT * FROM "employees" ORDER BY "name" ASC LIMIT ?', values: [50] }]);
	});

	it('builds SELECT without LIMIT when returnAll is true', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: true, where: { values: [] }, sort: { values: [] }, 'options.outputColumns': ['*'], 'options.selectDistinct': false, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT * FROM "employees"', values: [] }]);
	});

	it('generates one query per input item', async () => {
		const run = mockRunner();
		await select.execute.call(
			makeMock({ table: 'employees', returnAll: true, where: { values: [] }, sort: { values: [] }, 'options.outputColumns': ['*'], 'options.selectDistinct': false, combineConditions: 'AND' }),
			[item(), item()],
			run,
		);
		expect(run.mock.calls[0][0]).toHaveLength(2);
	});
});

// ─── insert ──────────────────────────────────────────────────────────────────

describe('insert.operation', () => {
	it('AUTO_MAP single item — builds INSERT with positional values', async () => {
		const run = mockRunner();
		await insert.execute.call(
			makeMock({ table: 'employees', dataMode: 'autoMapInputData' }),
			[item({ name: 'Alice', age: 30 })],
			run,
			{},
		);
		const [{ query, values }] = run.mock.calls[0][0];
		expect(query).toMatch(/^INSERT\s+INTO "employees"/);
		expect(query).toContain('"name"');
		expect(query).toContain('"age"');
		expect(values).toContain('Alice');
		expect(values).toContain(30);
	});

	it('AUTO_MAP multiple items — builds single batch INSERT', async () => {
		const run = mockRunner();
		await insert.execute.call(
			makeMock({ table: 'employees', dataMode: 'autoMapInputData' }),
			[item({ name: 'Alice' }), item({ name: 'Bob' })],
			run,
			{},
		);
		const [{ query, values }] = run.mock.calls[0][0];
		expect(query).toBe('INSERT  INTO "employees" ("name") VALUES (?),(?)');
		expect(values).toEqual(['Alice', 'Bob']);
	});

	it('MANUAL mode — builds INSERT from valuesToSend', async () => {
		const run = mockRunner();
		await insert.execute.call(
			makeMock({
				table: 'employees',
				dataMode: 'defineBelow',
				valuesToSend: { values: [{ column: 'name', value: 'Alice' }] },
			}),
			[item()],
			run,
			{},
		);
		const [{ query, values }] = run.mock.calls[0][0];
		expect(query).toBe('INSERT  INTO "employees" ("name") VALUES (?)');
		expect(values).toEqual(['Alice']);
	});

	it('uses OR IGNORE when skipOnConflict is set', async () => {
		const run = mockRunner();
		await insert.execute.call(
			makeMock({ table: 'employees', dataMode: 'autoMapInputData' }),
			[item({ name: 'Alice' })],
			run,
			{ skipOnConflict: true },
		);
		const [{ query }] = run.mock.calls[0][0];
		expect(query).toContain('OR IGNORE');
	});

	it('INDEPENDENTLY mode — builds one INSERT per item', async () => {
		const run = mockRunner();
		await insert.execute.call(
			makeMock({ table: 'employees', dataMode: 'autoMapInputData', options: {} }),
			[item({ name: 'Alice' }), item({ name: 'Bob' })],
			run,
			{ queryBatching: 'independently' },
		);
		expect(run.mock.calls[0][0]).toHaveLength(2);
	});
});

// ─── update ──────────────────────────────────────────────────────────────────

describe('update.operation', () => {
	it('AUTO_MAP — builds UPDATE with match column', async () => {
		const run = mockRunner();
		await update.execute.call(
			makeMock({ table: 'employees', columnToMatchOn: 'id', dataMode: 'autoMapInputData' }),
			[item({ id: 1, name: 'Alice' })],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([{
			query: 'UPDATE "employees" SET "name" = ? WHERE "id" = ?',
			values: ['Alice', 1],
		}]);
	});

	it('MANUAL mode — builds UPDATE from valuesToSend', async () => {
		const run = mockRunner();
		await update.execute.call(
			makeMock({
				table: 'employees',
				columnToMatchOn: 'id',
				dataMode: 'defineBelow',
				valueToMatchOn: '1',
				valuesToSend: { values: [{ column: 'name', value: 'Alice' }] },
			}),
			[item()],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([{
			query: 'UPDATE "employees" SET "name" = ? WHERE "id" = ?',
			values: ['Alice', '1'],
		}]);
	});

	it('skips item when no columns to update', async () => {
		const run = mockRunner();
		await update.execute.call(
			makeMock({ table: 'employees', columnToMatchOn: 'id', dataMode: 'autoMapInputData' }),
			[item({ id: 1 })],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([]);
	});

	it('generates one query per input item', async () => {
		const run = mockRunner();
		await update.execute.call(
			makeMock({ table: 'employees', columnToMatchOn: 'id', dataMode: 'autoMapInputData' }),
			[item({ id: 1, name: 'Alice' }), item({ id: 2, name: 'Bob' })],
			run,
			{},
		);
		expect(run.mock.calls[0][0]).toHaveLength(2);
	});
});

// ─── delete ──────────────────────────────────────────────────────────────────

describe('delete.operation', () => {
	it('delete command with no conditions — DELETE without WHERE', async () => {
		const run = mockRunner();
		await deleteOp.execute.call(
			makeMock({ table: 'employees', deleteCommand: 'delete', where: { values: [] }, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'DELETE FROM "employees"', values: [] }]);
	});

	it('delete command with WHERE clause', async () => {
		const run = mockRunner();
		await deleteOp.execute.call(
			makeMock({ table: 'employees', deleteCommand: 'delete', where: { values: [{ column: 'id', condition: 'equal', value: 42 }] }, combineConditions: 'AND' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'DELETE FROM "employees" WHERE "id" = ?', values: [42] }]);
	});

	it('truncate command — DELETE without WHERE', async () => {
		const run = mockRunner();
		await deleteOp.execute.call(
			makeMock({ table: 'employees', deleteCommand: 'truncate' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'DELETE FROM "employees"', values: [] }]);
	});

	it('drop command — DROP TABLE IF EXISTS', async () => {
		const run = mockRunner();
		await deleteOp.execute.call(
			makeMock({ table: 'employees', deleteCommand: 'drop' }),
			[item()],
			run,
		);
		expect(run).toHaveBeenCalledWith([{ query: 'DROP TABLE IF EXISTS "employees"', values: [] }]);
	});
});

// ─── upsert ──────────────────────────────────────────────────────────────────

describe('upsert.operation', () => {
	it('AUTO_MAP — builds INSERT … ON CONFLICT … DO UPDATE SET', async () => {
		const run = mockRunner();
		await upsert.execute.call(
			makeMock({ table: 'employees', columnToMatchOn: 'id', dataMode: 'autoMapInputData' }),
			[item({ id: 1, name: 'Alice' })],
			run,
			{},
		);
		const [{ query, values }] = run.mock.calls[0][0];
		expect(query).toMatch(/^INSERT INTO "employees"/);
		expect(query).toContain('ON CONFLICT("id") DO UPDATE SET');
		expect(query).toContain('"name" = excluded."name"');
		expect(values).toEqual([1, 'Alice']);
	});

	it('MANUAL mode — builds upsert from valuesToSend + valueToMatchOn', async () => {
		const run = mockRunner();
		await upsert.execute.call(
			makeMock({
				table: 'employees',
				columnToMatchOn: 'id',
				dataMode: 'defineBelow',
				valueToMatchOn: '1',
				valuesToSend: { values: [{ column: 'name', value: 'Alice' }] },
			}),
			[item()],
			run,
			{},
		);
		const [{ query }] = run.mock.calls[0][0];
		expect(query).toContain('ON CONFLICT("id") DO UPDATE SET');
		expect(query).toContain('"name" = excluded."name"');
	});

	it('replaces empty strings with nulls when option is set', async () => {
		const run = mockRunner();
		await upsert.execute.call(
			makeMock({ table: 'employees', columnToMatchOn: 'id', dataMode: 'autoMapInputData' }),
			[item({ id: 1, name: '' })],
			run,
			{ replaceEmptyStrings: true },
		);
		const [{ values }] = run.mock.calls[0][0];
		expect(values).toContain(null);
	});
});

// ─── executeQuery ─────────────────────────────────────────────────────────────

describe('executeQuery.operation', () => {
	it('passes raw query through unchanged when no params', async () => {
		const run = mockRunner();
		await executeQuery.execute.call(
			makeMock({ query: 'SELECT * FROM employees', options: {} }),
			[item()],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT * FROM employees', values: [] }]);
	});

	it('replaces $1 $2 with ? and collects ordered values', async () => {
		const run = mockRunner();
		await executeQuery.execute.call(
			makeMock({ query: 'SELECT * FROM employees WHERE id = $1 AND name = $2', options: { queryReplacement: [42, 'Alice'] } }),
			[item()],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([{
			query: 'SELECT * FROM employees WHERE id = ? AND name = ?',
			values: [42, 'Alice'],
		}]);
	});

	it('accepts comma-separated string for queryReplacement', async () => {
		const run = mockRunner();
		await executeQuery.execute.call(
			makeMock({ query: 'SELECT * FROM t WHERE id = $1', options: { queryReplacement: '99' } }),
			[item()],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT * FROM t WHERE id = ?', values: ['99'] }]);
	});

	it('treats null queryReplacement as empty params', async () => {
		const run = mockRunner();
		await executeQuery.execute.call(
			makeMock({ query: 'SELECT 1', options: { queryReplacement: null } }),
			[item()],
			run,
			{},
		);
		expect(run).toHaveBeenCalledWith([{ query: 'SELECT 1', values: [] }]);
	});

	it('throws NodeOperationError when queryReplacement is not an array or string', async () => {
		const run = mockRunner();
		await expect(
			executeQuery.execute.call(
				makeMock({ query: 'SELECT 1', options: { queryReplacement: 123 } }),
				[item()],
				run,
				{},
			),
		).rejects.toThrow(NodeOperationError);
	});
});
