<script lang="ts">
	import { browser } from '$app/environment';
	import {
		Plus,
		Search,
		QrCode,
		Pencil,
		Route,
		RotateCcw,
		Trash2,
		ToggleLeft,
		ToggleRight,
		AlertCircle
	} from 'lucide-svelte';
	import { t, i18n } from '$lib/i18n';
	import {
		api,
		ApiError,
		gbToBytes,
		keyStatus,
		bytesUsed,
		type KeyDTO,
		type KeyStatus,
		type NodeDTO,
		type CreateKeyResult,
		type RotateKeyResult
	} from '$lib/api';
	import { formatBytes, formatDate, shortId } from '$lib/format';
	import Card from '$lib/components/Card.svelte';
	import Button from '$lib/components/Button.svelte';
	import Badge from '$lib/components/Badge.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import QRModal from '$lib/components/QRModal.svelte';
	import TokenReveal from '$lib/components/TokenReveal.svelte';
	import { toast } from '$lib/state/toast.svelte';
	import { confirmDialog } from '$lib/state/confirm.svelte';

	const PAGE_SIZE = 50;
	const STATUSES: KeyStatus[] = ['active', 'disabled', 'over_quota', 'expired'];

	const statusLabel: Record<KeyStatus, string> = {
		active: 'generic.statusActive',
		disabled: 'generic.statusDisabled',
		over_quota: 'generic.statusOverQuota',
		expired: 'generic.statusExpired'
	};
	const statusTone: Record<KeyStatus, 'ok' | 'muted' | 'danger' | 'warn'> = {
		active: 'ok',
		disabled: 'muted',
		over_quota: 'danger',
		expired: 'warn'
	};

	// create form
	let label = $state('');
	let docUrl = $state('');
	let docUrls = $state('');
	let transport = $state('yandex');
	let e2eEncryption = $state(false);
	let limitGb = $state('');
	let ownerRef = $state('');
	let creating = $state(false);
	const isMultistream = $derived(transport === 'yandex_multistream');

	// result (fresh token / deeplink)
	let result = $state<CreateKeyResult | RotateKeyResult | null>(null);
	let qrOpen = $state(false);
	let createError = $state<string | null>(null);

	// list
	let keys = $state<KeyDTO[]>([]);
	let loading = $state(false);
	let search = $state('');
	let ownerFilter = $state('');
	let statusFilter = $state<'all' | KeyStatus>('all');
	let page = $state(0);
	let busyMap = $state<Record<string, boolean>>({});

	// limit modal
	let limitKey = $state<KeyDTO | null>(null);
	let limitInput = $state('');
	let limitBusy = $state(false);

	// cascade modal
	let nodes = $state<NodeDTO[]>([]);
	let cascadeKey = $state<KeyDTO | null>(null);
	let cascadeSelection = $state('');
	let cascadeBusy = $state(false);
	const cascadeTargets = $derived(
		nodes.filter((n) => n.PublicAddress && n.ID !== cascadeKey?.AssignedNodeID)
	);

	function errText(e: unknown): string {
		if (e instanceof ApiError) {
			if (e.message === 'network_error') return t('generic.error');
			if (e.message === 'unauthorized') return t('login.error');
			return e.message;
		}
		return t('generic.error');
	}

	async function loadKeys() {
		loading = true;
		try {
			keys = await api.listKeys({
				owner_ref: ownerFilter || undefined,
				limit: PAGE_SIZE,
				offset: page * PAGE_SIZE
			});
		} catch {
			keys = [];
		} finally {
			loading = false;
		}
	}

	async function loadNodes() {
		try {
			nodes = await api.listNodes();
		} catch {
			nodes = [];
		}
	}

	$effect(() => {
		if (!browser) return;
		loadKeys();
		loadNodes();
	});

	$effect(() => {
		ownerFilter;
		page;
		if (browser) loadKeys();
	});

	async function create() {
		if (creating) return;
		let parsedDocUrls: string[] = [];
		if (isMultistream) {
			parsedDocUrls = docUrls
				.split('\n')
				.map((s) => s.trim())
				.filter((s) => s);
			if (parsedDocUrls.length < 2) {
				createError = t('keys.invalidDocUrls');
				return;
			}
		} else if (!docUrl.trim()) {
			createError = t('keys.invalidDoc');
			return;
		}
		creating = true;
		createError = null;
		try {
			result = await api.createKey({
				label: label.trim() || undefined,
				doc_url: isMultistream ? undefined : docUrl.trim(),
				doc_urls: isMultistream ? parsedDocUrls : undefined,
				transport: transport || undefined,
				e2e_encryption: e2eEncryption,
				traffic_limit_bytes: gbToBytes(limitGb),
				owner_ref: ownerRef.trim() || undefined
			});
			toast.ok(t('keys.createSuccess'));
			label = '';
			docUrl = '';
			docUrls = '';
			ownerRef = '';
			limitGb = '';
			e2eEncryption = false;
			await loadKeys();
		} catch (e) {
			createError = errText(e);
		} finally {
			creating = false;
		}
	}

	function toggleEnabled(k: KeyDTO) {
		busyMap = { ...busyMap, [k.ID]: true };
		api
			.setKeyEnabled(k.ID, !k.Enabled)
			.then(async () => {
				await loadKeys();
			})
			.catch((e) => toast.error(errText(e)))
			.finally(() => {
				busyMap = { ...busyMap, [k.ID]: false };
			});
	}

	function openLimit(k: KeyDTO) {
		limitKey = k;
		limitInput = k.TrafficLimitBytes != null ? String(Math.round(k.TrafficLimitBytes / (1024 ** 3) * 10) / 10) : '';
	}

	function saveLimit() {
		if (!limitKey || limitBusy) return;
		limitBusy = true;
		const input = limitInput.trim();
		let bytes: number | null;
		if (input === '') {
			bytes = null;
		} else {
			const g = parseFloat(input);
			if (isNaN(g) || g <= 0) {
				toast.error(t('keys.limitGbShort'));
				limitBusy = false;
				return;
			}
			bytes = Math.round(g * 1024 ** 3);
		}
		api
			.patchKeyLimit(limitKey.ID, bytes)
			.then(async () => {
				toast.ok(t('keys.limitSaved'));
				limitKey = null;
				await loadKeys();
			})
			.catch((e) => toast.error(errText(e)))
			.finally(() => {
				limitBusy = false;
			});
	}

	function openCascade(k: KeyDTO) {
		cascadeKey = k;
		cascadeSelection = k.FinalExitNodeID ?? '';
	}

	function saveCascade() {
		if (!cascadeKey || cascadeBusy) return;
		cascadeBusy = true;
		api
			.patchKeyFinalExit(cascadeKey.ID, cascadeSelection || null)
			.then(async () => {
				toast.ok(t('keys.cascadeUpdated'));
				cascadeKey = null;
				await loadKeys();
			})
			.catch((e) => {
				if (e instanceof ApiError && e.status === 409) {
					toast.error(t('keys.cascadeNoAddress'));
				} else {
					toast.error(errText(e));
				}
			})
			.finally(() => {
				cascadeBusy = false;
			});
	}

	function confirmDelete(k: KeyDTO) {
		confirmDialog.ask({
			title: t('keys.deleteConfirmTitle'),
			message: k.Label ? `${t('keys.colLabel')}: ${k.Label}` : undefined,
			confirmLabel: t('keys.delete'),
			tone: 'danger',
			onConfirm: async () => {
				await api.deleteKey(k.ID);
				toast.ok(t('keys.deleted'));
				await loadKeys();
			}
		});
	}

	function confirmRotate(k: KeyDTO) {
		confirmDialog.ask({
			title: t('keys.rotateConfirmTitle'),
			message: t('keys.rotateConfirmMsg'),
			confirmLabel: t('keys.rotate'),
			tone: 'accent',
			onConfirm: async () => {
				const res = await api.rotateKeyToken(k.ID);
				result = res;
				toast.ok(t('keys.rotated'));
				await loadKeys();
			}
		});
	}

	function resetFilters() {
		search = '';
		ownerFilter = '';
		statusFilter = 'all';
		page = 0;
	}

	const filtered = $derived.by(() => {
		let rows = keys;
		const q = search.trim().toLowerCase();
		if (q) rows = rows.filter((k) => k.Label.toLowerCase().includes(q) || k.ID.toLowerCase().includes(q));
		if (statusFilter !== 'all') rows = rows.filter((k) => keyStatus(k) === statusFilter);
		return rows;
	});
</script>

<svelte:head>
	<title>OpenFlux · {t('nav.keys')}</title>
</svelte:head>

<div class="space-y-6">
	<div class="flex items-center justify-between gap-3">
		<h1 class="text-xl font-semibold text-[var(--of-ink)]">{t('keys.title')}</h1>
	</div>

	<!-- Create -->
	<Card title={t('keys.createTitle')}>
		<form
			class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4"
			onsubmit={(e) => {
				e.preventDefault();
				create();
			}}
		>
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.label')}</span>
				<input class="input" bind:value={label} placeholder={t('keys.labelPh')} />
			</label>
			{#if isMultistream}
				<label class="block">
					<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.docUrls')} *</span>
					<textarea class="input" rows="3" bind:value={docUrls} placeholder={t('keys.docUrlsPh')}></textarea>
				</label>
			{:else}
				<label class="block">
					<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.docUrl')} *</span>
					<input class="input" bind:value={docUrl} placeholder={t('keys.docUrlPh')} />
				</label>
			{/if}
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.transport')}</span>
				<select class="input" bind:value={transport}>
					<option value="yandex">yandex</option>
					<option value="yandex_multistream">yandex_multistream</option>
					<option value="boards">boards</option>
					<option value="mailru">mailru</option>
					<option value="direct">direct</option>
				</select>
			</label>
			<label class="block">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.limitGb')}</span>
				<input class="input" type="number" min="0" step="0.1" bind:value={limitGb} />
			</label>
			<label class="flex items-end gap-2 pb-2">
				<input type="checkbox" class="h-4 w-4" bind:checked={e2eEncryption} />
				<span class="text-xs font-medium text-[var(--of-muted)]">{t('keys.e2e')}</span>
			</label>
			<label class="block sm:col-span-2 lg:col-span-2">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.owner')}</span>
				<input class="input" bind:value={ownerRef} placeholder={t('keys.ownerPh')} />
			</label>
			<div class="flex items-end sm:col-span-2 lg:col-span-2">
				<Button type="submit" variant="primary" disabled={creating} class="w-full">
					<Plus class="h-4 w-4" />
					{creating ? t('generic.loading') : t('keys.create')}
				</Button>
			</div>
		</form>

		{#if createError}
			<p class="mt-3 flex items-center gap-1.5 text-sm text-[var(--of-danger)]">
				<AlertCircle class="h-4 w-4" />
				{createError}
			</p>
		{/if}

		{#if result && (result as RotateKeyResult & CreateKeyResult).deep_link}
			<div class="mt-4 space-y-3 rounded-xl border border-[var(--of-line)] bg-[var(--of-raise)] p-4">
				<div class="flex items-center justify-between gap-2">
					<h3 class="text-sm font-semibold text-[var(--of-ink)]">{t('keys.createdTitle')}</h3>
					<Button size="sm" variant="ghost" onclick={() => (qrOpen = true)}>
						<QrCode class="h-4 w-4" />
						{t('keys.qr')}
					</Button>
				</div>
				<TokenReveal label={t('keys.token')} value={(result as RotateKeyResult & CreateKeyResult).token} />
				<TokenReveal label={t('keys.connectLink')} value={(result as RotateKeyResult & CreateKeyResult).deep_link!} />
			</div>
		{/if}
	</Card>

	<!-- List -->
	<Card title={t('keys.listTitle')}>
		<div class="mb-4 grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto_auto]">
			<div class="relative">
				<Search class="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-[var(--of-muted)]" />
				<input class="input !pl-9" bind:value={search} placeholder={t('keys.searchPh')} />
			</div>
			<input class="input" bind:value={ownerFilter} placeholder={t('keys.filterOwnerPh')} />
			<select class="input" bind:value={statusFilter}>
				<option value="all">{t('keys.statusAll')}</option>
				{#each STATUSES as s (s)}
					<option value={s}>{t(statusLabel[s])}</option>
				{/each}
			</select>
			<Button size="sm" variant="ghost" onclick={resetFilters}>
				{t('keys.reset')}
			</Button>
		</div>

		<div class="overflow-x-auto">
			<table class="of-table">
				<thead>
					<tr>
						<th>{t('keys.colLabel')}</th>
						<th>{t('keys.colId')}</th>
						<th>{t('keys.colTransport')}</th>
						<th>{t('keys.colStatus')}</th>
						<th class="num">{t('keys.colTraffic')}</th>
						<th>{t('keys.colOwner')}</th>
						<th>{t('keys.colLastSeen')}</th>
						<th>{t('keys.colActions')}</th>
					</tr>
				</thead>
				<tbody>
					{#each filtered as k (k.ID)}
						{@const status = keyStatus(k)}
						<tr>
							<td>
								<span class="font-medium text-[var(--of-ink)]">{k.Label || '—'}</span>
							</td>
							<td class="font-mono text-xs text-[var(--of-muted)]" title={k.ID}>{shortId(k.ID)}</td>
							<td>
								<span class="text-xs text-[var(--of-muted)]">{k.Transport}</span>
							</td>
							<td>
								<Badge tone={statusTone[status]} dot>{t(statusLabel[status])}</Badge>
							</td>
							<td class="num tabular-nums text-[var(--of-ink)]">
								{formatBytes(bytesUsed(k), i18n.lang)}
								{#if k.TrafficLimitBytes != null}
									<span class="text-[var(--of-muted)]"> / {formatBytes(k.TrafficLimitBytes, i18n.lang)}</span>
								{/if}
							</td>
							<td class="text-xs text-[var(--of-muted)]">{k.OwnerRef || '—'}</td>
							<td class="text-xs text-[var(--of-muted)]">{formatDate(k.LastSeenAt, i18n.lang)}</td>
							<td>
								<div class="flex items-center gap-1">
									{#if k.Enabled}
										<button
											type="button"
											class="of-iconbtn"
											title={t('keys.disable')}
											disabled={busyMap[k.ID]}
											onclick={() => toggleEnabled(k)}
										>
											<ToggleLeft class="h-4 w-4" />
										</button>
									{:else}
										<button
											type="button"
											class="of-iconbtn"
											title={t('keys.enable')}
											disabled={busyMap[k.ID]}
											onclick={() => toggleEnabled(k)}
										>
											<ToggleRight class="h-4 w-4" />
										</button>
									{/if}
									<button type="button" class="of-iconbtn" title={t('keys.setLimit')} onclick={() => openLimit(k)}>
										<Pencil class="h-4 w-4" />
									</button>
									<button
										type="button"
										class="of-iconbtn {k.FinalExitNodeID ? '!text-[var(--of-accent)]' : ''}"
										title={t('keys.cascade')}
										onclick={() => openCascade(k)}
									>
										<Route class="h-4 w-4" />
									</button>
									<button
										type="button"
										class="of-iconbtn"
										title={t('keys.rotate')}
										onclick={() => confirmRotate(k)}
									>
										<RotateCcw class="h-4 w-4" />
									</button>
									<button type="button" class="of-iconbtn !text-[var(--of-danger)]" title={t('keys.delete')} onclick={() => confirmDelete(k)}>
										<Trash2 class="h-4 w-4" />
									</button>
								</div>
							</td>
						</tr>
					{:else}
						<tr>
							<td colspan="8" class="py-8 text-center text-[var(--of-muted)]">
								{keys.length === 0 ? t('keys.empty') : t('keys.emptyFiltered')}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		<div class="mt-4 flex items-center justify-between gap-3">
			<span class="text-xs text-[var(--of-muted)]">
				{loading ? t('generic.loading') : t('generic.showing', { from: String(page * PAGE_SIZE + 1), to: String(page * PAGE_SIZE + filtered.length), total: String(page * PAGE_SIZE + keys.length) })}
			</span>
			<div class="flex items-center gap-2">
				<Button size="sm" disabled={page === 0} onclick={() => (page = page - 1)}>
					{t('keys.prev')}
				</Button>
				<span class="text-xs text-[var(--of-muted)]">{t('keys.pages', { page: String(page + 1) })}</span>
				<Button size="sm" disabled={keys.length < PAGE_SIZE} onclick={() => (page = page + 1)}>
					{t('keys.next')}
				</Button>
			</div>
		</div>
	</Card>

	<!-- Limit modal -->
	<Modal
		open={!!limitKey}
		title={t('keys.setLimitTitle')}
		onClose={() => (limitKey = null)}
	>
		<label class="block">
			<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.limitGbShort')}</span>
			<input class="input" type="number" min="0" step="0.1" bind:value={limitInput} placeholder={t('keys.setLimitPh')} />
		</label>
		{#snippet footer()}
			<Button size="sm" onclick={() => (limitKey = null)} disabled={limitBusy}>
				{t('generic.cancel')}
			</Button>
			<Button size="sm" variant="primary" onclick={saveLimit} disabled={limitBusy}>
				{limitBusy ? t('generic.loading') : t('generic.confirm')}
			</Button>
		{/snippet}
	</Modal>

	<!-- Cascade modal -->
	<Modal
		open={!!cascadeKey}
		title={t('keys.cascade')}
		onClose={() => (cascadeKey = null)}
	>
		<p class="mb-3 text-xs text-[var(--of-muted)]">{t('keys.cascadeHint')}</p>
		<label class="block">
			<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('keys.cascade')}</span>
			<select class="input" bind:value={cascadeSelection}>
				<option value="">{t('keys.cascadeDirect')}</option>
				{#each cascadeTargets as n (n.ID)}
					<option value={n.ID}>{n.Name}</option>
				{/each}
			</select>
		</label>
		{#snippet footer()}
			<Button size="sm" onclick={() => (cascadeKey = null)} disabled={cascadeBusy}>
				{t('generic.cancel')}
			</Button>
			<Button size="sm" variant="primary" onclick={saveCascade} disabled={cascadeBusy}>
				{cascadeBusy ? t('generic.loading') : t('generic.confirm')}
			</Button>
		{/snippet}
	</Modal>

	<!-- QR for the last-created/rotated link -->
	<QRModal
		open={qrOpen}
		text={result && (result as RotateKeyResult & CreateKeyResult).deep_link ? (result as RotateKeyResult & CreateKeyResult).deep_link! : ''}
		title={t('keys.qrTitle')}
		hint={t('keys.qrHint')}
		onClose={() => (qrOpen = false)}
	/>
</div>