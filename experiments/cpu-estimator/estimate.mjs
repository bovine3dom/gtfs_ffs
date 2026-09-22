// Rough process CPU-ms; fitted by fit.jl. Assumes a valid router URL and a warm cache miss.
export function estimateCpuMs(url) {
  const p = new URL(url, 'https://router.invalid').searchParams;
  const n = (key, fallback) => Number(p.get(key) ?? fallback);
  const pop = p.get('metric') === 'accessible_population', r = pop ? n('origin_radius', 0) : 0;
  const res = p.has('index') ? parseInt(p.get('index')[1], 16) : (n('index_upper', 8 << 20) >>> 20) & 15;
  const b = n('budget_h', 100), w = Math.min(b, n('max_walk_h', 1));
  const window = n('window_h', 0), step = n('step_h', 1 / 60);
  const s = window > 0 && step > 0 ? Math.ceil(window / step) : 1;
  return 6.21 * (p.get('network') === 'rail_and_friends' ? 0.477 : 1) * 1.59 ** (res - 8)
    * (pop ? 0.977 : 1) * (1 + 3 * r * (r + 1)) ** 0.4
    * (1 + Math.log1p(b)) ** 1.84 * (1 + w) ** 0.0049 * s ** 0.467;
}
