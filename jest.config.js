/** @type {import('jest').Config} */
module.exports = {
	preset: 'ts-jest',
	testEnvironment: 'node',
	testMatch: ['**/__tests__/**/*.test.ts'],
	moduleFileExtensions: ['ts', 'js'],
	collectCoverageFrom: ['nodes/**/*.ts', '!nodes/**/__tests__/**', '!nodes/**/*.d.ts'],
};
