import { escapeSqlIdentifier, prepareQueryAndReplacements, addWhereClauses, addSortRules } from '../utils';
import type { WhereClause, SortRule } from '../interfaces';

describe('SQLite Node v2 utils', () => {
	describe('escapeSqlIdentifier', () => {
		it('wraps identifier in double quotes', () => {
			expect(escapeSqlIdentifier('users')).toBe('"users"');
		});

		it('escapes double quotes inside identifier', () => {
			expect(escapeSqlIdentifier('"users"')).toBe('"""users"""');
		});
	});

	describe('prepareQueryAndReplacements', () => {
		it('replaces $1, $2 with ? and collects values', () => {
			const { query, values } = prepareQueryAndReplacements('SELECT * FROM t WHERE a = $1 AND b = $2', ['x', 42]);
			expect(query).toBe('SELECT * FROM t WHERE a = ? AND b = ?');
			expect(values).toEqual(['x', 42]);
		});

		it('returns empty values when no placeholders', () => {
			const { query, values } = prepareQueryAndReplacements('SELECT 1', []);
			expect(query).toBe('SELECT 1');
			expect(values).toEqual([]);
		});
	});

	describe('addWhereClauses', () => {
		const node = {} as Parameters<typeof addWhereClauses>[0];

		it('returns unchanged query when no clauses', () => {
			const [query, values] = addWhereClauses(node, 0, 'SELECT * FROM t', [], []);
			expect(query).toBe('SELECT * FROM t');
			expect(values).toEqual([]);
		});

		it('adds WHERE with single clause', () => {
			const clauses: WhereClause[] = [{ column: 'id', condition: '=', value: 1 }];
			const [query, values] = addWhereClauses(node, 0, 'SELECT * FROM t', clauses, []);
			expect(query).toContain('WHERE');
			expect(query).toContain('"id"');
			expect(values).toEqual([1]);
		});
	});

	describe('addSortRules', () => {
		it('returns unchanged query when no rules', () => {
			const [query, values] = addSortRules('SELECT * FROM t', [], []);
			expect(query).toBe('SELECT * FROM t');
			expect(values).toEqual([]);
		});

		it('adds ORDER BY', () => {
			const rules: SortRule[] = [{ column: 'name', direction: 'ASC' }];
			const [query] = addSortRules('SELECT * FROM t', rules, []);
			expect(query).toContain('ORDER BY');
			expect(query).toContain('"name"');
			expect(query).toContain('ASC');
		});
	});
});
