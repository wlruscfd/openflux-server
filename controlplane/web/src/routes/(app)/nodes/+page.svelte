<script lang="ts">
	import { browser } from '$app/environment';
	import { Plus, RotateCcw, Server, AlertCircle } from 'lucide-svelte';
	import { t, i18n } from '$lib/i18n';
	import { api, ApiError, type NodeDTO, type CreateNodeResult } from '$lib/api';
	import { formatDate, shortId, timeAgoMs } from '$lib/format';
	import Card from '$lib/components/Card.svelte';
	import Button from '$lib/components/Button.svelte';
	import Badge from '$lib/components/Badge.svelte';
	import TokenReveal from '$lib/components/TokenReveal.svelte';
	import { toast } from '$lib/state/toast.svelte';
	import { confirmDialog } from '$lib/state/confirm.svelte';

	const ONLINE_WINDOW_MS = 3 * 60 * 1000;

	let name = $state('');
	let maxKeys = $state('999999');
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let createdToken = $state<CreateNodeResult | null>(null);
	let nodes = $state<NodeDTO[]>([]);
	let loading = $state(false);
	let busyKey = $state('');
	let addressDrafts = $state<Record<string, string>>({});

	function errText(e: unknown): string {
		if (e instanceof ApiError) {
			if (e.message === 'network_error') return t('generic.error');
			return e.message;
		}
		return t('generic.error');
	}

	async function load() {
		loading = true;
		try {
			nodes = await api.listNodes();
			for (const n of nodes) {
				if (!(n.ID in addressDrafts)) addressDrafts[n.ID] = n.PublicAddress ?? '';
			}
		} catch (e) {
			toast.error(errText(e));
			nodes = [];
		} finally {
			loading = false;
		}
	}

	async function saveAddress(node: NodeDTO) {
		const value = (addressDrafts[node.ID] ?? '').trim();
		if (value === (node.PublicAddress ?? '')) return;
		busyKey = node.ID;
		try {
			await api.patchNode(node.ID, { public_address: value });
			toast.ok(t('nodes.publicAddressSaved'));
			await load();
		} catch (e) {
			toast.error(errText(e));
		} finally {
			busyKey = '';
		}
	}

	$effect(() => {
		if (browser) load();
	});

	async function createNode() {
		if (creating || !name.trim()) return;
		creating = true;
		createError = null;
		try {
			const parsed = parseInt(maxKeys, 10);
			createdToken = await api.createNode(name.trim(), isNaN(parsed) ? 999999 : parsed);
			toast.ok(t('nodes.registeredSuccess'));
			name = '';
			await load();
		} catch (e) {
			createError = errText(e);
		} finally {
			creating = false;
		}
	}

	function rotate(node: NodeDTO) {
		confirmDialog.ask({
			title: t('nodes.rotateConfirmTitle'),
			message: t('nodes.rotateConfirmMsg'),
			confirmLabel: t('nodes.rotate'),
			tone: 'accent',
			onConfirm: async () => {
				busyKey = node.ID;
				try {
					const res = await api.rotateNodeToken(node.ID);
					createdToken = { id: node.ID, name: node.Name, max_keys: node.MaxKeys, token: res.token };
					toast.ok(t('nodes.rotated'));
				} finally {
					busyKey = '';
				}
			}
		});
	}
</script>

<svelte:head>
	<title>OpenFlux · {t('nav.nodes')}</title>
</svelte:head>

<div class="space-y-6">
	<h1 class="text-xl font-semibold text-[var(--of-ink)]">{t('nodes.title')}</h1>

	<Card title={t('nodes.registerTitle')}>
		<form
			class="grid grid-cols-1 gap-3 sm:grid-cols-3"
			onsubmit={(e) => {
				e.preventDefault();
				createNode();
			}}
		>
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('nodes.name')} *</span>
				<input class="input" bind:value={name} placeholder={t('nodes.namePh')} />
			</label>
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('nodes.maxKeys')}</span>
				<input class="input" type="number" min="1" bind:value={maxKeys} />
			</label>
			<div class="flex items-end">
				<Button type="submit" variant="primary" disabled={creating || !name.trim()} class="w-full">
					<Plus class="h-4 w-4" />
					{creating ? t('generic.loading') : t('nodes.register')}
				</Button>
			</div>
		</form>

		{#if createError}
			<p class="mt-3 flex items-center gap-1.5 text-sm text-[var(--of-danger)]">
				<AlertCircle class="h-4 w-4" />
				{createError}
			</p>
		{/if}

		{#if createdToken}
			<div class="mt-4 space-y-3 rounded-xl border border-[var(--of-line)] bg-[var(--of-raise)] p-4">
				<h3 class="text-sm font-semibold text-[var(--of-ink)]">
					{t('nodes.newNodeTitle')} · {createdToken.name}
				</h3>
				<TokenReveal label={t('nodes.nodeToken')} value={createdToken.token} />
			</div>
		{/if}
	</Card>

	<Card title={t('nodes.title')}>
		<div class="overflow-x-auto">
			<table class="of-table of-stack">
				<thead>
					<tr>
						<th>{t('nodes.colName')}</th>
						<th>{t('nodes.colId')}</th>
						<th>{t('nodes.colStatus')}</th>
						<th class="num">{t('nodes.colKeys')}</th>
						<th>{t('nodes.colHeartbeat')}</th>
						<th>{t('nodes.colCreated')}</th>
						<th title={t('nodes.publicAddressHint')}>{t('nodes.colPublicAddress')}</th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					{#each nodes as node (node.ID)}
						{@const since = node.LastHeartbeatAt ? timeAgoMs(node.LastHeartbeatAt) : null}
						{@const online = since !== null && since <= ONLINE_WINDOW_MS}
						<tr>
							<td class="of-title">
								<div class="flex items-center gap-2">
									<Server class="h-4 w-4 shrink-0 text-[var(--of-muted)]" />
									<span class="font-medium text-[var(--of-ink)]">{node.Name}</span>
								</div>
							</td>
							<td class="font-mono text-xs text-[var(--of-muted)]" title={node.ID} data-label={t('nodes.colId')}>{shortId(node.ID)}</td>
							<td data-label={t('nodes.colStatus')}>
								{#if online}
									<Badge tone="ok" dot>{t('generic.online')}</Badge>
								{:else}
									<Badge tone="muted" dot>{t('generic.offline')}</Badge>
								{/if}
							</td>
							<td class="num tabular-nums text-[var(--of-ink)]" data-label={t('nodes.colKeys')}>
								{node.ActiveKeys}{node.MaxKeys > 0 ? ` / ${node.MaxKeys}` : ''}
							</td>
							<td class="text-xs text-[var(--of-muted)]" data-label={t('nodes.colHeartbeat')}>{formatDate(node.LastHeartbeatAt, i18n.lang)}</td>
							<td class="text-xs text-[var(--of-muted)]" data-label={t('nodes.colCreated')}>{formatDate(node.CreatedAt, i18n.lang)}</td>
							<td data-label={t('nodes.colPublicAddress')}>
								<div class="flex w-full items-center gap-1.5 sm:w-auto">
									<input
										class="input h-8 text-xs"
										placeholder={t('nodes.publicAddressPh')}
										bind:value={addressDrafts[node.ID]}
										onblur={() => saveAddress(node)}
										onkeydown={(e) => e.key === 'Enter' && saveAddress(node)}
										disabled={busyKey === node.ID}
									/>
								</div>
							</td>
							<td class="of-actions">
								<button
									type="button"
									class="of-iconbtn"
									title={t('nodes.rotate')}
									disabled={busyKey === node.ID}
									onclick={() => rotate(node)}
								>
									<RotateCcw class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{:else}
						<tr>
							<td colspan="8" class="py-8 text-center text-[var(--of-muted)]">
								{loading ? t('generic.loading') : t('nodes.empty')}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</Card>
</div>