<script lang="ts">
	import { browser } from '$app/environment';
	import { Plus, ToggleLeft, ToggleRight, PlugZap, AlertCircle } from 'lucide-svelte';
	import { t, i18n } from '$lib/i18n';
	import { api, ApiError, type IngestTokenDTO, type CreateIngestTokenResult } from '$lib/api';
	import { formatDate, shortId } from '$lib/format';
	import Card from '$lib/components/Card.svelte';
	import Button from '$lib/components/Button.svelte';
	import Badge from '$lib/components/Badge.svelte';
	import TokenReveal from '$lib/components/TokenReveal.svelte';
	import { toast } from '$lib/state/toast.svelte';

	let label = $state('');
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let created = $state<CreateIngestTokenResult | null>(null);
	let tokens = $state<IngestTokenDTO[]>([]);
	let loading = $state(false);
	let busyKey = $state('');

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
			tokens = await api.listIngestTokens();
		} catch (e) {
			toast.error(errText(e));
			tokens = [];
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		if (browser) load();
	});

	async function create() {
		if (creating || !label.trim()) return;
		creating = true;
		createError = null;
		try {
			created = await api.createIngestToken(label.trim());
			toast.ok(t('ingest.created'));
			label = '';
			await load();
		} catch (e) {
			createError = errText(e);
		} finally {
			creating = false;
		}
	}

	function toggle(token: IngestTokenDTO) {
		busyKey = token.ID;
		api
			.setIngestTokenEnabled(token.ID, !token.Enabled)
			.then(load)
			.catch((e) => toast.error(errText(e)))
			.finally(() => {
				busyKey = '';
			});
	}
</script>

<svelte:head>
	<title>OpenFlux · {t('nav.ingest')}</title>
</svelte:head>

<div class="space-y-6">
	<div>
		<h1 class="text-2xl font-semibold tracking-tight text-[var(--of-ink)]">{t('ingest.title')}</h1>
		<p class="mt-1 text-sm text-[var(--of-muted)]">{t('ingest.subtitle')}</p>
	</div>

	<Card title={t('ingest.createTitle')}>
		<form
			class="grid grid-cols-1 gap-3 sm:grid-cols-3"
			onsubmit={(e) => {
				e.preventDefault();
				create();
			}}
		>
			<label class="block sm:col-span-2">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('ingest.label')} *</span>
				<input class="input" bind:value={label} placeholder={t('ingest.labelPh')} />
			</label>
			<div class="flex items-end">
				<Button type="submit" variant="primary" disabled={creating || !label.trim()} class="w-full">
					<Plus class="h-4 w-4" />
					{creating ? t('generic.loading') : t('ingest.create')}
				</Button>
			</div>
		</form>

		{#if createError}
			<p class="mt-3 flex items-center gap-1.5 text-sm text-[var(--of-danger)]">
				<AlertCircle class="h-4 w-4" />
				{createError}
			</p>
		{/if}

		{#if created}
			<div class="mt-4 space-y-3 rounded-xl border border-[var(--of-line)] bg-[var(--of-raise)] p-4">
				<h3 class="text-sm font-semibold text-[var(--of-ink)]">
					{t('ingest.createdTitle')} · {created.label}
				</h3>
				<TokenReveal label={t('ingest.token')} value={created.token} />
			</div>
		{/if}
	</Card>

	<Card title={t('ingest.title')}>
		<div class="overflow-x-auto">
			<table class="of-table of-stack">
				<thead>
					<tr>
						<th>{t('ingest.colLabel')}</th>
						<th>{t('ingest.colId')}</th>
						<th>{t('ingest.colStatus')}</th>
						<th>{t('ingest.colScope')}</th>
						<th>{t('ingest.colCreated')}</th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					{#each tokens as token (token.ID)}
						<tr>
							<td class="of-title">
								<div class="flex items-center gap-2">
									<PlugZap class="h-4 w-4 shrink-0 text-[var(--of-muted)]" />
									<span class="font-medium text-[var(--of-ink)]">{token.Label}</span>
								</div>
							</td>
							<td class="font-mono text-xs text-[var(--of-muted)]" title={token.ID} data-label={t('ingest.colId')}>{shortId(token.ID)}</td>
							<td data-label={t('ingest.colStatus')}>
								{#if token.Enabled}
									<Badge tone="ok" dot>{t('generic.enabled')}</Badge>
								{:else}
									<Badge tone="muted" dot>{t('generic.disabled')}</Badge>
								{/if}
							</td>
							<td class="font-mono text-xs text-[var(--of-muted)]" data-label={t('ingest.colScope')}>{token.Scope}</td>
							<td class="text-xs text-[var(--of-muted)]" data-label={t('ingest.colCreated')}>{formatDate(token.CreatedAt, i18n.lang)}</td>
							<td class="of-actions">
								{#if token.Enabled}
									<button type="button" class="of-iconbtn" title={t('generic.disabled')} disabled={busyKey === token.ID} onclick={() => toggle(token)}>
										<ToggleLeft class="h-4 w-4" />
									</button>
								{:else}
									<button type="button" class="of-iconbtn" title={t('generic.enabled')} disabled={busyKey === token.ID} onclick={() => toggle(token)}>
										<ToggleRight class="h-4 w-4" />
									</button>
								{/if}
							</td>
						</tr>
					{:else}
						<tr>
							<td colspan="6" class="py-8 text-center text-[var(--of-muted)]">
								{loading ? t('generic.loading') : t('ingest.empty')}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</Card>
</div>