<script lang="ts">
	import { browser } from '$app/environment';
	import { Plus, Trash2, ShieldOff, AlertCircle } from 'lucide-svelte';
	import { t } from '$lib/i18n';
	import { api, ApiError, PROXY_CARRIERS, type ProxyKeyDTO } from '$lib/api';
	import { formatDate } from '$lib/format';
	import Card from '$lib/components/Card.svelte';
	import Button from '$lib/components/Button.svelte';
	import Badge from '$lib/components/Badge.svelte';
	import { toast } from '$lib/state/toast.svelte';
	import { confirmDialog } from '$lib/state/confirm.svelte';

	let link = $state('');
	let name = $state('');
	let priority = $state('1');
	let carriers = $state<string[]>([]);
	let keys = $state<ProxyKeyDTO[]>([]);
	let loading = $state(false);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let busyKey = $state('');
	let bulk = $state('');
	let importing = $state(false);
	let lastImport = $state('');

	async function importKeys() {
		if (importing || !bulk.trim() || carriers.length === 0) return;
		importing = true;
		lastImport = '';
		const parsed = parseInt(priority, 10);
		try {
			const res = await api.importProxyKeys({
				links: bulk,
				carriers,
				priority: isNaN(parsed) ? 1 : parsed
			});
			bulk = '';
			const parts = [`${t('proxy.bulkDone')} ${res.created}`];
			if (res.malformed) parts.push(`${t('proxy.bulkBad')} ${res.malformed}`);
			if (res.rejected) parts.push(`${t('proxy.bulkDropped')} ${res.rejected}`);
			if (res.full) parts.push(t('proxy.bulkFull'));
			lastImport = parts.join(' · ');
			toast.ok(lastImport);
			await load();
		} catch (e) {
			toast.error(errText(e));
		} finally {
			importing = false;
		}
	}

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
			keys = await api.listProxyKeys();
		} catch (e) {
			toast.error(errText(e));
			keys = [];
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		if (browser) load();
	});

	function toggleCarrier(c: string) {
		carriers = carriers.includes(c) ? carriers.filter((x) => x !== c) : [...carriers, c];
	}

	async function createKey() {
		if (creating || !link.trim()) return;
		creating = true;
		createError = null;
		const parsed = parseInt(priority, 10);
		try {
			await api.createProxyKey({
				link: link.trim(),
				name: name.trim() || undefined,
				priority: isNaN(parsed) ? 1 : parsed,
				carriers
			});
			toast.ok(t('proxy.created'));
			link = '';
			name = '';
			priority = '1';
			carriers = [];
			await load();
		} catch (e) {
			createError = errText(e);
		} finally {
			creating = false;
		}
	}

	async function setEnabled(k: ProxyKeyDTO, enabled: boolean) {
		busyKey = k.id;
		try {
			await api.patchProxyKey(k.id, { enabled });
			await load();
		} catch (e) {
			toast.error(errText(e));
		} finally {
			busyKey = '';
		}
	}

	function remove(k: ProxyKeyDTO) {
		confirmDialog.ask({
			title: t('proxy.deleteTitle'),
			message: t('proxy.deleteMsg'),
			confirmLabel: t('proxy.delete'),
			tone: 'danger',
			onConfirm: async () => {
				busyKey = k.id;
				try {
					await api.deleteProxyKey(k.id);
					toast.ok(t('proxy.deleted'));
					await load();
				} catch (e) {
					toast.error(errText(e));
				} finally {
					busyKey = '';
				}
			}
		});
	}
</script>

<svelte:head>
	<title>OpenFlux · {t('proxy.title')}</title>
</svelte:head>

<div class="space-y-6">
	<h1 class="text-2xl font-semibold tracking-tight text-[var(--of-ink)]">{t('proxy.title')}</h1>
	<p class="text-sm text-[var(--of-muted)]">{t('proxy.hint')}</p>

	<Card title={t('proxy.addTitle')}>
		<form
			class="grid grid-cols-1 gap-3 sm:grid-cols-3"
			onsubmit={(e) => {
				e.preventDefault();
				createKey();
			}}
		>
			<label class="block sm:col-span-2">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('proxy.link')} *</span>
				<input class="input font-mono text-xs" bind:value={link} placeholder={t('proxy.linkPh')} />
			</label>
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('proxy.name')}</span>
				<input class="input" bind:value={name} placeholder={t('proxy.namePh')} />
			</label>
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('proxy.priority')}</span>
				<input class="input" type="number" min="1" bind:value={priority} />
			</label>
			<div class="sm:col-span-2">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('proxy.carriers')}</span>
				<div class="flex flex-wrap gap-1.5">
					{#each PROXY_CARRIERS as c (c)}
						<button
							type="button"
							class="rounded-full px-2.5 py-0.5 text-xs font-medium whitespace-nowrap transition-colors
								{carriers.includes(c)
								? 'bg-[var(--of-accent)]/15 text-[var(--of-accent)]'
								: 'bg-[var(--of-raise)] text-[var(--of-muted)]'}"
							onclick={() => toggleCarrier(c)}
						>
							{c}
						</button>
					{/each}
				</div>
			</div>
			<div class="flex items-end">
				<Button type="submit" variant="primary" disabled={creating || !link.trim()} class="w-full">
					<Plus class="h-4 w-4" />
					{creating ? t('generic.loading') : t('proxy.add')}
				</Button>
			</div>
			<div class="sm:col-span-3">
				<label class="block">
					<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('proxy.bulk')}</span>
					<textarea
						class="input h-24 font-mono text-xs"
						bind:value={bulk}
						placeholder={t('proxy.bulkPh')}
					></textarea>
				</label>
				<div class="mt-2 flex items-center gap-2">
					<Button
						type="button"
						variant="secondary"
						disabled={importing || !bulk.trim() || carriers.length === 0}
						onclick={importKeys}
					>
						{importing ? t('generic.loading') : t('proxy.bulkAdd')}
					</Button>
					{#if lastImport}
						<span class="text-xs text-[var(--of-muted)]">{lastImport}</span>
					{/if}
				</div>
			</div>
		</form>

		{#if createError}
			<p class="mt-3 flex items-center gap-1.5 text-sm text-[var(--of-danger)]">
				<AlertCircle class="h-4 w-4" />
				{createError}
			</p>
		{/if}
	</Card>

	<Card title={t('proxy.listTitle')}>
		<div class="overflow-x-auto">
			<table class="of-table of-stack">
				<thead>
					<tr>
						<th>{t('proxy.colName')}</th>
						<th>{t('proxy.colKind')}</th>
						<th>{t('proxy.colServer')}</th>
						<th>{t('proxy.colCarriers')}</th>
						<th class="num">{t('proxy.colPriority')}</th>
						<th>{t('proxy.colStatus')}</th>
						<th>{t('proxy.colState')}</th>
						<th>{t('proxy.colCreated')}</th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					{#each keys as k (k.id)}
						<tr>
							<td class="of-title font-medium text-[var(--of-ink)]" data-label={t('proxy.colName')}>
								{k.name || t('proxy.unnamed')}
							</td>
							<td data-label={t('proxy.colKind')}>
								<Badge tone="muted">{k.kind}</Badge>
							</td>
							<td class="font-mono text-xs text-[var(--of-muted)]" data-label={t('proxy.colServer')}>
								{k.server}:{k.port}
							</td>
							<td data-label={t('proxy.colCarriers')}>
								<div class="flex flex-wrap gap-1">
									{#each k.carriers as c (c)}
										<Badge tone="muted">{c}</Badge>
									{/each}
								</div>
							</td>
							<td class="num tabular-nums text-[var(--of-ink)]" data-label={t('proxy.colPriority')}>
								{k.priority}
							</td>
							<td data-label={t('proxy.colStatus')}>
								{#if k.enabled}
									<Badge tone="ok" dot>{t('generic.enabled')}</Badge>
								{:else}
									<Badge tone="muted" dot>{t('generic.disabled')}</Badge>
								{/if}
							</td>
							<td data-label={t('proxy.colState')}>
								{#if k.state === 'ok'}
									<Badge tone="ok" dot>
										{t('proxy.stateOk')}{k.state_latency_ms ? ` · ${k.state_latency_ms} ms` : ''}
									</Badge>
								{:else if k.state === 'failed'}
									<span title={k.state_error ?? ''}>
										<Badge tone="danger" dot>{t('proxy.stateFailed')}</Badge>
									</span>
								{:else}
									<Badge tone="muted" dot>{t('proxy.stateUnknown')}</Badge>
								{/if}
							</td>
							<td class="text-xs text-[var(--of-muted)]" data-label={t('proxy.colCreated')}>
								{formatDate(k.created_at)}
							</td>
							<td class="of-actions">
								<button
									type="button"
									class="of-iconbtn"
									title={k.enabled ? t('proxy.disable') : t('proxy.enable')}
									disabled={busyKey === k.id}
									onclick={() => setEnabled(k, !k.enabled)}
								>
									<ShieldOff class="h-4 w-4" />
								</button>
								<button
									type="button"
									class="of-iconbtn"
									title={t('proxy.delete')}
									disabled={busyKey === k.id}
									onclick={() => remove(k)}
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{:else}
						<tr>
							<td colspan="8" class="py-8 text-center text-[var(--of-muted)]">
								{loading ? t('generic.loading') : t('proxy.empty')}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</Card>
</div>