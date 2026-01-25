import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

// ==== CONFIG ====

const NODES = new SharedArray('nodes', () => [
    'http://127.0.0.1:3001',
    'http://127.0.0.1:3002',
    'http://127.0.0.1:3003',
    'http://127.0.0.1:3004',
    'http://127.0.0.1:3005',
]);

// "Clients" and duration
export const options = {
    vus: 50,              // Multiple VUs
    duration: '10s',     // Standard benchmark duration
};

// Read/write mix (e.g. 0.7 = 70% reads, 30% writes)
const READ_RATIO = 0.5;

const ITEMS_PATH = '/items';

// ==== SHARED STATE ====

// Monotonically growing counter for write keys
// Using a simple approach: each VU tracks its own counter and we use VU id + counter to create unique keys
// For reads, we use the global max written key across all VUs (approximated)

// Track the highest key written (shared across VUs via execution context)
let maxWrittenKey = -1;

// Custom metrics to track operations
const writeOps = new Counter('write_operations');
const readOps = new Counter('read_operations');

// ==== HELPERS ====

function randomInt(max) {
    return Math.floor(Math.random() * max);
}

function pickNode() {
    const idx = randomInt(NODES.length);
    return NODES[idx];
}

function randomValue() {
    return `val-${randomInt(1_000_000_000)}`;
}

// ==== MAIN VU FUNCTION ====

export default function () {
    const node = pickNode();

    // Decide whether to read or write
    // If no keys written yet, always write first
    const shouldRead = maxWrittenKey >= 0 && Math.random() < READ_RATIO;

    if (shouldRead) {
        // READ: GET {node}/items/{key} using a random key from 0 to maxWrittenKey
        const key = randomInt(maxWrittenKey + 1).toString();
        const url = `${node}${ITEMS_PATH}/${key}`;
        const res = http.get(url);

        check(res, {
            'read status is 200': r => r.status === 200,
        });

        readOps.add(1);
    } else {
        // WRITE: POST {node}/items with incrementing key
        maxWrittenKey++;
        const key = maxWrittenKey.toString();

        const url = `${node}${ITEMS_PATH}`;
        const body = JSON.stringify({
            partition_key: key,
            message: randomValue(),
        });

        const res = http.post(url, body, {
            headers: { 'Content-Type': 'application/json' },
        });

        check(res, {
            'write status is 2xx/3xx': r => r.status >= 200 && r.status < 400,
        });

        writeOps.add(1);
    }
}
