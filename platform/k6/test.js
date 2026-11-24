// Test de carga de tu API. En la evaluación usamos este mismo script con los parámetros del dataset de evaluación.
//   make load-test            (o: docker compose --profile test run --rm load-test)
import http from 'k6/http';
import { check } from 'k6';

const P = JSON.parse(open('./params.json'));
const BASE = __ENV.TARGET_URL || 'http://localhost:5000';

export const options = {
  scenarios: {
    warmup: { executor: 'constant-vus', vus: 5, duration: '15s', tags: { phase: 'warmup' } },
    load: {
      executor: 'ramping-vus',
      startTime: '15s',
      stages: [
        { duration: '20s', target: 50 },
        { duration: '40s', target: 150 },
        { duration: '10s', target: 0 },
      ],
      tags: { phase: 'load' },
    },
  },
  thresholds: {
    'http_req_failed{phase:load}': ['rate<0.01'],
    'http_req_duration{phase:load}': ['p(95)<300'],
  },
};

function pick(a) {
  return a[Math.floor(Math.random() * a.length)];
}

function addDays(iso, n) {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

// Ventanas semanales dentro del periodo.
const WINDOWS = [];
for (let s = P.period.start; addDays(s, 6) <= P.period.end; s = addDays(s, 7)) {
  WINDOWS.push({ from: s, to: addDays(s, 6) });
}

export default function () {
  const w = pick(WINDOWS);
  const r = Math.random();
  let res;
  if (r < 0.45) {
    res = http.get(`${BASE}/v1/kpis/daily?country=${pick(P.countries)}&from=${w.from}&to=${w.to}`, { tags: { name: 'kpis_daily' } });
    check(res, {
      'kpis_daily 200': (x) => x.status === 200,
      'kpis_daily 7 días': (x) => { try { return x.json('days').length === 7; } catch (e) { return false; } },
    });
  } else if (r < 0.85) {
    res = http.get(`${BASE}/v1/stores/${pick(P.stores)}/kpis?from=${w.from}&to=${w.to}`, { tags: { name: 'store_kpis' } });
    check(res, { 'store_kpis 200': (x) => x.status === 200 });
  } else {
    res = http.get(`${BASE}/v1/products/top?country=${pick(P.countries)}&from=${w.from}&to=${w.to}&limit=10`, { tags: { name: 'top_products' } });
    check(res, { 'top_products 200': (x) => x.status === 200 });
  }
}
