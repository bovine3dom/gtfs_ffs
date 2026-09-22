import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { estimateCpuMs } from './estimate.mjs';

const base = '/reachable?index=881fb46625fffff&departure_h=5&budget_h=100';
test('small standalone module with one numeric API', async () => {
  const source = readFileSync(new URL('./estimate.mjs', import.meta.url));
  assert.ok(source.length < 1500);
  assert.ok(source.toString().trim().split('\n').length <= 15);
  const module = await import(`data:text/javascript;base64,${source.toString('base64')}`);
  assert.deepEqual(Object.keys(module), ['estimateCpuMs']);
  assert.ok(module.estimateCpuMs(base) > 0);
});
test('100-hour defaults, full URLs, and query-only URLs', () => {
  assert.equal(estimateCpuMs(base), estimateCpuMs('https://example.com' + base));
  assert.equal(estimateCpuMs(base), estimateCpuMs(base.replace('/reachable', '')));
  assert.equal(estimateCpuMs(base), estimateCpuMs(base.replace('&budget_h=100', '')));
  assert.ok(Number.isFinite(estimateCpuMs('/reachable')));
});
test('split words and canonical H3 strings have the same resolution', () => {
  const h = 0x881fb46625fffffn;
  const split = base.replace('index=881fb46625fffff', `index_lower=${h & 4294967295n}&index_upper=${h >> 32n}`);
  assert.equal(estimateCpuMs(split), estimateCpuMs(base));
});
test('extrapolation does not reject networks, locations, resolutions, or long budgets', () => {
  for (const budget of [0, 6, 100, 168, 1000]) {
    for (const index of ['801ffffffffffff', '851fb467fffffff', '881fb46625fffff', '8f28308280f18f2']) {
      const url = `?index=${index}&departure_h=5&budget_h=${budget}&network=unknown&max_walk_h=2&metric=accessible_population&origin_radius=30&window_h=24&step_h=0.001`;
      assert.ok(estimateCpuMs(url) > 0 && Number.isFinite(estimateCpuMs(url)));
    }
  }
  assert.equal(estimateCpuMs(base + '&network=unknown'), estimateCpuMs(base));
  assert.ok(estimateCpuMs(base.replace('100', '168')) >= estimateCpuMs(base));
});
test('ignored options and zero step have no cost effect', () => {
  for (const suffix of ['&coarseness=3', '&origin_radius=18', '&exclude_origin_population=true',
    '&encoding=string&distance_mode=straight_line', '&window_h=12&step_h=0', '&window_mode=reachable_union'])
    assert.equal(estimateCpuMs(base + suffix), estimateCpuMs(base));
  const pop = base + '&metric=accessible_population';
  assert.ok(estimateCpuMs(pop + '&origin_radius=18') >= estimateCpuMs(pop));
});
test('sample count uses ceil and collapses a step larger than the window', () => {
  assert.equal(estimateCpuMs(base + '&window_h=1&step_h=2'), estimateCpuMs(base));
  assert.equal(estimateCpuMs(base + '&window_h=1&step_h=0.3'), estimateCpuMs(base + '&window_h=4&step_h=1'));
});
test('all recorded URLs are positive and match the shipped-coefficient report', () => {
  const lines = readFileSync(new URL('./validation.csv', import.meta.url), 'utf8').trim().split('\n').slice(1);
  assert.ok(lines.length >= 384);
  for (const line of lines) {
    const row = line.split(','), actual = estimateCpuMs(row[6]);
    assert.ok(actual > 0 && Number.isFinite(actual));
    assert.ok(Math.abs(actual / Number(row[4]) - 1) < 1e-10, row[6]);
  }
});
