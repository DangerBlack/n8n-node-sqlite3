import { SqliteV1 } from '../SqliteV1.node';
import { NodeOperationError } from 'n8n-workflow';

jest.mock('better-sqlite3', () => {
	return jest.fn().mockImplementation(() => ({
		prepare: jest.fn().mockImplementation(() => ({
			all: jest.fn().mockReturnValue([{ id: 1, name: 'Alice' }]),
			run: jest.fn().mockReturnValue({ changes: 1, lastInsertRowid: 1 }),
		})),
		exec: jest.fn(),
		close: jest.fn(),
	}));
});

jest.mock('fs', () => ({
	existsSync: jest.fn().mockReturnValue(true),
	mkdirSync: jest.fn(),
}));

function getMockExecuteFunctions(params: Record<string, unknown>) {
	return {
		getInputData: () => [{}],
		getNodeParameter: (name: string, _itemIndex: number, defaultValue?: unknown) =>
			params[name] ?? defaultValue,
		continueOnFail: () => false,
		getNode: () => ({}),
	} as unknown as import('n8n-workflow').IExecuteFunctions;
}

describe('SqliteV1', () => {
	it('should execute SELECT query', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual([{ id: 1, name: 'Alice' }]);
	});

	it('should execute SELECT query with multiple queries', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees; SELECT * FROM departments WHERE id = @id',
			args: '{"id": 1}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: {},
		}));
		expect(result).toEqual([[{ json: [[{ id: 1, name: 'Alice' }], [{ id: 1, name: 'Alice' }]], pairedItem: { item: 0 } }]]);
	});

	it('should execute SELECT query and spread results', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'SELECT',
			spread: true,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual({ id: 1, name: 'Alice' });
	});

	it('should execute INSERT query', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'INSERT INTO employees (name) VALUES (@name)',
			args: '{"name": "Bob"}',
			query_type: 'INSERT',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual({ changes: 1, last_id: 1 });
	});

	it('should filter out unused args', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'INSERT INTO employees (name) VALUES (@name)',
			args: '{"name": "Bob", "unused_param": 123}',
			query_type: 'INSERT',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual({ changes: 1, last_id: 1 });
	});

	it('should execute UPDATE query', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'UPDATE employees SET name = @name WHERE id = @id',
			args: '{"name": "Charlie", "id": 1}',
			query_type: 'UPDATE',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual({ changes: 1, last_id: 1 });
	});

	it('should execute DELETE query', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'DELETE FROM employees WHERE id = @id',
			args: '{"id": 1}',
			query_type: 'DELETE',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual({ changes: 1, last_id: 1 });
	});

	it('should execute CREATE query', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'CREATE TABLE employees (id INTEGER PRIMARY KEY)',
			args: '{}',
			query_type: 'CREATE',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual({ message: 'Query executed successfully.' });
	});

	it('should auto-detect SELECT query type', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'AUTO',
			spread: false,
			additionalOptions: {},
		}));
		expect(result[0][0].json).toEqual([{ id: 1, name: 'Alice' }]);
	});

	it('should auto-detect query type for various SQL commands', async () => {
		const cases = [
			{ query: 'SELECT * FROM employees', expected: [{ id: 1, name: 'Alice' }] },
			{ query: 'INSERT INTO employees (name) VALUES (@name)', expected: { changes: 1, last_id: 1 } },
			{ query: 'UPDATE employees SET name = @name WHERE id = @id', expected: { changes: 1, last_id: 1 } },
			{ query: 'DELETE FROM employees WHERE id = @id', expected: { changes: 1, last_id: 1 } },
			{ query: 'CREATE TABLE employees (id INTEGER PRIMARY KEY)', expected: { message: 'Query executed successfully.' } },
			{ query: '.schema', expected: { message: 'Query executed successfully.' } },
		];

		for (const { query, expected } of cases) {
			const node = new SqliteV1();
			const result = await node.execute.call(getMockExecuteFunctions({
				db_path: 'test.sqlite',
				query,
				args: '{"name":"Test","id":1,"unused_param":123}',
				query_type: 'AUTO',
				spread: false,
				additionalOptions: {},
			}));
			expect(result[0][0].json).toEqual(expected);
		}
	});

	it('should throw error if db_path is missing', async () => {
		const node = new SqliteV1();
		await expect(node.execute.call(getMockExecuteFunctions({
			db_path: '',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: {},
		}))).rejects.toThrow(NodeOperationError);
	});

	it('should throw error if query is missing', async () => {
		const node = new SqliteV1();
		await expect(node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: '',
			args: '{}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: {},
		}))).rejects.toThrow(NodeOperationError);
	});

	it('should throw error if args is invalid JSON', async () => {
		const node = new SqliteV1();
		await expect(node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: 'not-json',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: {},
		}))).rejects.toThrow(NodeOperationError);
	});

	it('should use custom bindings path when file exists', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: { use_custom_bindings: '/custom/path/to/bindings' },
		}));
		expect(result[0][0].json).toEqual([{ id: 1, name: 'Alice' }]);
	});

	it('should throw if custom bindings file is not found', async () => {
		const fs = require('fs');
		const original = fs.existsSync;
		fs.existsSync = jest.fn().mockReturnValue(false);

		const node = new SqliteV1();
		await expect(node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: { use_custom_bindings: '/custom/path/to/bindings' },
		}))).rejects.toThrow(NodeOperationError);

		fs.existsSync = original;
	});

	it('should use default bindings when use_default_bindings is true', async () => {
		const node = new SqliteV1();
		const result = await node.execute.call(getMockExecuteFunctions({
			db_path: 'test.sqlite',
			query: 'SELECT * FROM employees',
			args: '{}',
			query_type: 'SELECT',
			spread: false,
			additionalOptions: { use_default_bindings: true },
		}));
		expect(result[0][0].json).toEqual([{ id: 1, name: 'Alice' }]);
	});
});
