// Custom production entry: proxies /v1/* and /healthz to the Go controlplane (skipped when nginx does it) and redirects the bare site root to /admin/.

import { createServer } from 'node:http';
import { handler } from './build/handler.js';

const PORT = Number(
	process.env.CONTROLPLANE_WEB_PORT || process.env.PORT || 3000
);
const HOST = process.env.CONTROLPLANE_WEB_HOST || process.env.HOST || '127.0.0.1';
const UPSTREAM = (
	process.env.CONTROLPLANE_UPSTREAM ||
	process.env.UPSTREAM ||
	'http://127.0.0.1:8080'
).replace(/\/+$/, '');

const isProxyPath = (pathname) => pathname === '/healthz' || pathname.startsWith('/v1/');

function readBody(req) {
	return new Promise((resolve) => {
		const chunks = [];
		req.on('data', (c) => chunks.push(c));
		req.on('end', () => resolve(Buffer.concat(chunks)));
	});
}

function writeJSON(res, status, obj) {
	const body = JSON.stringify(obj);
	res.writeHead(status, {
		'content-type': 'application/json; charset=utf-8',
		'content-length': Buffer.byteLength(body)
	});
	res.end(body);
}

async function proxy(req, res) {
	const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

	try {
		const body = await readBody(req);
		const headers = new Headers();
		for (const [name, value] of Object.entries(req.headers)) {
			if (value == null || name === 'host' || name === 'connection') continue;
			if (Array.isArray(value)) {
				for (const v of value) headers.append(name, v);
			} else {
				headers.set(name, String(value));
			}
		}
		headers.set('content-length', String(body.byteLength));
		headers.set('x-forwarded-proto', url.protocol.replace(':', ''));
		const peer = (req.socket.remoteAddress || '').replace(/^::ffff:/, '');
		if (peer && peer !== '127.0.0.1' && peer !== '::1') {
			headers.set('x-real-ip', peer);
			headers.delete('x-forwarded-for');
		}

		const resUpstream = await fetch(`${UPSTREAM}${url.pathname}${url.search}`, {
			method: req.method,
			headers,
			body: req.method === 'GET' || req.method === 'HEAD' ? undefined : body
		});

		const outBody = Buffer.from(await resUpstream.arrayBuffer());
		res.writeHead(resUpstream.status, {
			'content-type': resUpstream.headers.get('content-type') ?? 'application/json'
		});
		res.end(outBody);
	} catch (err) {
		writeJSON(res, 502, { error: `upstream unreachable: ${UPSTREAM}` });
	}
}

const server = createServer((req, res) => {
	const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

	if (isProxyPath(url.pathname)) {
		return proxy(req, res);
	}

	if (url.pathname === '/') {
		res.writeHead(308, { location: '/admin/' });
		return res.end();
	}

	handler(req, res);
});

server.on('clientError', (err, socket) => {
	socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
});

server.listen(PORT, HOST, () => {
	console.log(`openflux-web serving /admin on http://${HOST}:${PORT}, /v1/* -> ${UPSTREAM}`);
});