import { goto } from '$app/navigation';
import { base } from '$app/paths';
import { auth } from './state/auth.svelte';

// ---- API wire models ----------------------------------------------------

// Note: list endpoints use the Go structs' capitalized field names; create/stats endpoints use lowercase snake_case.

export interface NodeDTO {
	ID: string;
	Name: string;
	MaxKeys: number;
	Status: string;
	LastHeartbeatAt: string | null;
	CreatedAt: string;
	ActiveKeys: number;
	PublicAddress: string | null;
}

export interface KeyDTO {
	ID: string;
	Label: string;
	Transport: string;
	DocURL: string;
	// Set instead of DocURL for Transport "yandex_multistream" (2+ URLs).
	DocURLs: string[] | null;
	E2EEncryption: boolean;
	AssignedNodeID: string | null;
	Enabled: boolean;
	TrafficLimitBytes: number | null;
	BytesSentTotal: number;
	BytesReceivedTotal: number;
	OwnerRef: string;
	ExpiresAt: string | null;
	CreatedAt: string;
	UpdatedAt: string;
	LastSeenAt: string | null;
	FinalExitNodeID: string | null;
	RelayPort: number | null;
}

export type KeyStatus = 'active' | 'disabled' | 'over_quota' | 'expired';

export function keyStatus(k: KeyDTO, now = Date.now()): KeyStatus {
	if (!k.Enabled) return 'disabled';
	if (k.ExpiresAt && now > new Date(k.ExpiresAt).getTime()) return 'expired';
	if (k.TrafficLimitBytes != null && k.BytesSentTotal + k.BytesReceivedTotal >= k.TrafficLimitBytes) {
		return 'over_quota';
	}
	return 'active';
}

export function bytesUsed(k: KeyDTO): number {
	return (k.BytesSentTotal || 0) + (k.BytesReceivedTotal || 0);
}

export interface IngestTokenDTO {
	ID: string;
	Label: string;
	Scope: string;
	Enabled: boolean;
	CreatedAt: string;
}

export interface CreateKeyResult {
	id: string;
	token: string;
	deep_link?: string;
}

export interface RotateKeyResult {
	token: string;
	deep_link?: string;
}

export interface CreateNodeResult {
	id: string;
	name: string;
	max_keys: number;
	token: string;
}

export interface CreateIngestTokenResult {
	id: string;
	label: string;
	scope: string;
	token: string;
}

export interface StatsSummaryDTO {
	total_bytes_sent: number;
	total_bytes_received: number;
	today_bytes_sent: number;
	today_bytes_received: number;
	total_keys: number;
	enabled_keys: number;
	over_quota_keys: number;
	expired_keys: number;
	total_nodes: number;
	online_nodes: number;
}

export interface UsageDayDTO {
	day: string;
	bytes_sent: number;
	bytes_received: number;
	active_keys: number;
}

export interface SystemInfoDTO {
	hostname: string;
	num_cpu: number;
	load1: number;
	load5: number;
	load15: number;
	uptime_sec: number;
	cpu_percent: number;
	mem_total: number;
	mem_used: number;
	swap_total: number;
	swap_used: number;
	disk_total: number;
	disk_used: number;
	process_cpu_percent: number;
}

// ---- client -------------------------------------------------------------

export class ApiError extends Error {
	status: number;
	constructor(status: number, message: string) {
		super(message);
		this.status = status;
	}
}

export function isLoginPage(): boolean {
	return location.pathname.replace(/\/+$/, '') === `${base}/login`;
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
	const headers: Record<string, string> = {};
	if (auth.token) headers['Authorization'] = `Bearer ${auth.token}`;
	if (body !== undefined) headers['Content-Type'] = 'application/json';

	let res: Response;
	try {
		res = await fetch(path, {
			method,
			headers,
			body: body !== undefined ? JSON.stringify(body) : undefined
		});
	} catch {
		throw new ApiError(0, 'network_error');
	}

	if (res.status === 401) {
		auth.clear();
		if (!isLoginPage()) {
			await goto(`${base}/login`);
		}
		throw new ApiError(401, 'unauthorized');
	}

	if (res.status === 204) return undefined as T;

	const data = await res.json().catch(() => null);
	if (!res.ok) {
		const msg = data && typeof data.error === 'string' ? (data.error as string) : `HTTP ${res.status}`;
		throw new ApiError(res.status, msg);
	}
	return data as T;
}

export const api = {
	// nodes
	listNodes: () => request<NodeDTO[]>('GET', '/v1/admin/nodes'),
	createNode: (name: string, maxKeys: number) =>
		request<CreateNodeResult>('POST', '/v1/admin/nodes', { name, max_keys: maxKeys }),
	rotateNodeToken: (id: string) => request<{ token: string }>('POST', `/v1/admin/nodes/${id}/rotate-token`),
	patchNode: (id: string, body: { max_keys?: number; public_address?: string }) =>
		request('PATCH', `/v1/admin/nodes/${id}`, body),

	// keys
	listKeys: (params: { owner_ref?: string; limit?: number; offset?: number } = {}) => {
		const q = new URLSearchParams();
		if (params.owner_ref) q.set('owner_ref', params.owner_ref);
		if (params.limit != null) q.set('limit', String(params.limit));
		if (params.offset != null) q.set('offset', String(params.offset));
		const qs = q.toString();
		return request<KeyDTO[]>('GET', `/v1/admin/keys${qs ? `?${qs}` : ''}`);
	},
	createKey: (body: {
		label?: string;
		doc_url?: string;
		doc_urls?: string[];
		transport?: string;
		e2e_encryption?: boolean;
		traffic_limit_bytes?: number;
		owner_ref?: string;
	}) => request<CreateKeyResult>('POST', '/v1/admin/keys', body),
	getKey: (id: string) => request<KeyDTO>('GET', `/v1/admin/keys/${id}`),
	setKeyEnabled: (id: string, enabled: boolean) =>
		request('POST', `/v1/admin/keys/${id}/${enabled ? 'enable' : 'disable'}`),
	rotateKeyToken: (id: string) =>
		request<RotateKeyResult>('POST', `/v1/admin/keys/${id}/rotate-token`),
	patchKey: (
		id: string,
		body: {
			label?: string;
			transport?: string;
			doc_url?: string;
			doc_urls?: string[];
			e2e_encryption?: boolean;
			traffic_limit_bytes?: number | null;
			owner_ref?: string;
			expires_at?: string | null;
			final_exit_node_id?: string | null;
			enabled?: boolean;
		},
	) => request<KeyDTO>('PATCH', `/v1/admin/keys/${id}`, body),
	patchKeyLimit: (id: string, limitBytes: number | null) =>
		request('PATCH', `/v1/admin/keys/${id}`, { traffic_limit_bytes: limitBytes }),
	patchKeyFinalExit: (id: string, finalExitNodeId: string | null) =>
		request<KeyDTO>('PATCH', `/v1/admin/keys/${id}/final-exit`, { final_exit_node_id: finalExitNodeId }),
	deleteKey: (id: string) => request<null>('DELETE', `/v1/admin/keys/${id}`),
	keyUsage: (id: string, days: number) =>
		request<UsageDayDTO[]>('GET', `/v1/admin/keys/${id}/usage?days=${days}`),

	// ingest tokens
	listIngestTokens: () => request<IngestTokenDTO[]>('GET', '/v1/admin/ingest-tokens'),
	createIngestToken: (label: string, scope = 'keys:write') =>
		request<CreateIngestTokenResult>('POST', '/v1/admin/ingest-tokens', { label, scope }),
	setIngestTokenEnabled: (id: string, enabled: boolean) =>
		request('POST', `/v1/admin/ingest-tokens/${id}/${enabled ? 'enable' : 'disable'}`),

	// stats & system
	statsSummary: () => request<StatsSummaryDTO>('GET', '/v1/admin/stats/summary'),
	statsUsage: (days: number) => request<UsageDayDTO[]>('GET', `/v1/admin/stats/usage?days=${days}`),
	system: () => request<SystemInfoDTO>('GET', '/v1/admin/system')
};

// Helpers for file/bytes inputs on the create form.
export function gbToBytes(gb: string | number | null | undefined): number | undefined {
	if (gb === undefined || gb === null || gb === '') return undefined;
	const v = typeof gb === 'number' ? gb : parseFloat(gb);
	if (isNaN(v) || v <= 0) return undefined;
	return Math.round(v * 1024 * 1024 * 1024);
}