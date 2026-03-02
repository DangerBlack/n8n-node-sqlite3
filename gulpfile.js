const path = require('path');
const { task, src, dest } = require('gulp');

task('build:icons', copyIcons);

function copyIcons() {
	const nodeSource = path.resolve('nodes', '**', '*.{png,svg}');
	const nodeDestination = path.resolve('dist', 'nodes');

	src(nodeSource).pipe(dest(nodeDestination));

	// Copy node icon to dist/nodes/assets (resolved from v2 as ../../assets/ -> dist/nodes/assets)
	const nodesAssetsDest = path.resolve('dist', 'nodes', 'assets');
	src(path.resolve('nodes', 'SqliteNode', 'sqlite-icon.svg')).pipe(dest(nodesAssetsDest));

	const credSource = path.resolve('credentials', '**', '*.{png,svg}');
	const credDestination = path.resolve('dist', 'credentials');

	return src(credSource).pipe(dest(credDestination));
}
