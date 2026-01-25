import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { sleep } from 'k6';

// ==== CONFIG ====

const NODES = new SharedArray('nodes', () => [
    'http://127.0.0.1:3001',
    'http://127.0.0.1:3002',
    'http://127.0.0.1:3003',
]);

// "Clients" and duration
export const options = {
    vus: 3,              // roughly: N clients
    duration: '10s',       // how long to run
};

// read/write mix (e.g. 0.7 = 70% reads, 30% writes)
const READ_RATIO = 0.7;
const SLEEP = 10;

// Endpoint patterns – adjust to your API
const READ_PATH = '/items';        // GET base + /kv/{key}
const WRITE_PATH = '/items';       // POST base + /kv

// ==== HELPERS ====

function randomInt(max) {
    return Math.floor(Math.random() * max);
}

function randomKey() {
    return `key-${randomInt(1_000_000)}-${randomInt(1_000_000)}`;
}

function randomValue() {
    return `val-${randomInt(1_000_000_000)}`;
}

function pickNode() {
    const idx = randomInt(NODES.length);
    return NODES[idx];
}

// ==== MAIN VU FUNCTION ====

export default function () {
    const node = pickNode();
    const isRead = Math.random() < READ_RATIO;
    const key = randomKey();

    if (false) {
        // READ: GET {node}/kv/{key}
        const url = `${node}${READ_PATH}/${key}`;
        const res = http.get(url);

        check(res, {
            'read status is 2xx/3xx': r => r.status >= 200 && r.status < 400,
        });
    } else {
        // WRITE: POST {node}/kv with JSON body
        const url = `${node}${WRITE_PATH}`;
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
    }
}
