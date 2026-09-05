import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {join, resolve} from 'node:path';

const [frontend, directory] = process.argv.slice(2);
const require = createRequire(resolve(frontend, 'package.json'));
const {ArrowLoader} = require('@loaders.gl/arrow');
const {getResolution, splitLongToH3Index} = require('h3-js');
const columns = {};

for (const encoding of ['string', 'split']) {
    const bytes = readFileSync(join(directory, `${encoding}.arrow`));
    const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    const table = ArrowLoader.parseSync(buffer, {arrow: {shape: 'arrow-table'}}).data;
    columns[encoding] = Object.fromEntries(table.schema.fields.map(field => [
        field.name, table.getChild(field.name).toArray()
    ]));
    assert(columns[encoding].value instanceof Float64Array);
    assert(columns[encoding].elapsed_ms instanceof Uint32Array);
    assert.deepEqual(Array.from(columns[encoding].value).sort((a, b) => a - b), [0, 20, 40, 60]);
}

const {index_lower: lower, index_upper: upper} = columns.split;
assert(lower instanceof Uint32Array);
assert(upper instanceof Uint32Array);
const indices = Array.from(lower, (word, i) => splitLongToH3Index(word, upper[i]));
assert.deepEqual(indices, Array.from(columns.string.index));
assert(indices.every(index => getResolution(index) === 5));
assert.deepEqual(columns.string.elapsed_ms, columns.split.elapsed_ms);
console.log('H3-MON ArrowLoader: emitted string and split IPC files are compatible');
