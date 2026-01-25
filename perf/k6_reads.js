import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

// ==== CONFIG ====

const NODES = new SharedArray('nodes', () => [
    'http://127.0.0.1:3001',
    'http://127.0.0.1:3002',
    'http://127.0.0.1:3003',
]);

// "Clients" and duration
export const options = {
    vus: 3,              // Multiple VUs to measure throughput
    duration: '10s',       // Standard benchmark duration
};

const READ_PATH = '/items';

// ==== HELPERS ====

function randomInt(max) {
    return Math.floor(Math.random() * max);
}

function pickNode() {
    const idx = randomInt(NODES.length);
    return NODES[idx];
}

// ==== MAIN VU FUNCTION ====

export default function () {
    const node = pickNode();
    // write_items.sh inserts keys from 0 to 999
    const key = randomInt(1000).toString();

    const url = `${node}${READ_PATH}/${key}`;
    const res = http.get(url);

    check(res, {
        'read status is 200': r => r.status === 200,
    });
}
