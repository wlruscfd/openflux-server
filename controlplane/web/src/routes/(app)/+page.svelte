<script lang="ts">
	import { browser } from '$app/environment';
	import {
		ArrowUp,
		ArrowDown,
		KeyRound,
		AlertTriangle,
		Server,
		Users
	} from 'lucide-svelte';
	import { t, i18n } from '$lib/i18n';
	import { api, type StatsSummaryDTO, type UsageDayDTO, type NodeDTO, type SystemInfoDTO } from '$lib/api';
	import { formatBytes, formatDate, timeAgoMs } from '$lib/format';
	import Card from '$lib/components/Card.svelte';
	import StatCard from '$lib/components/StatCard.svelte';
	import Badge from '$lib/components/Badge.svelte';
	import UsageChart from '$lib/components/UsageChart.svelte';
	import SystemLoadCard from '$lib/components/SystemLoadCard.svelte';

	const ONLINE_WINDOW_MS = 3 * 60 * 1000;
	const DAYS_OPTIONS = [7, 30, 90, 365];

	let summary = $state<StatsSummaryDTO | null>(null);
	let usage = $state<UsageDayDTO[]>([]);
	let nodes = $state<NodeDTO[]>([]);
	let system = $state<SystemInfoDTO | null>(null);
	let loadError = $state(false);
	let usageDays = $state<number>(30);

	async function loadAll() {
		try {
			const [s, n, sys] = await Promise.all([
				api.statsSummary(),
				api.listNodes(),
				api.system().catch(() => null)
			]);
			summary = s;
			nodes = n;
			system = sys;
			loadError = false;
		} catch {
			loadError = true;
		}
	}

	async function loadUsage() {
		try {
			usage = await api.statsUsage(usageDays);
		} catch {
			usage = [];
		}
	}

	$effect(() => {
		if (!browser) return;
		loadAll();
		const id = setInterval(loadAll, 15000);
		return () => clearInterval(id);
	});

	$effect(() => {
		usageDays;
		if (browser) loadUsage();
	});

	const chartData = $derived(usage.map((d) => ({ day: d.day, sent: d.bytes_sent, recv: d.bytes_received })));
	const todayTraffic = $derived((summary?.today_bytes_sent ?? 0) + (summary?.today_bytes_received ?? 0));
	const totalTraffic = $derived((summary?.total_bytes_sent ?? 0) + (summary?.total_bytes_received ?? 0));
</script>

<svelte:head>
	<title>OpenFlux · {t('nav.dashboard')}</title>
</svelte:head>

<div class="space-y-6">
	<div class="flex items-center justify-between gap-3">
		<h1 class="text-xl font-semibold text-[var(--of-ink)]">{t('dash.overview')}</h1>
		{#if loadError}
			<Badge tone="danger" dot>{t('generic.error')}</Badge>
		{/if}
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 gap-3 lg:grid-cols-5 [&>*:last-child:nth-child(odd)]:col-span-2 lg:[&>*:last-child:nth-child(odd)]:col-span-1">
		<StatCard
			label={t('dash.totalTraffic')}
			value={formatBytes(totalTraffic, i18n.lang)}
			sub={`${formatBytes(summary?.total_bytes_sent ?? null, i18n.lang)} ${t('dash.up')} / ${formatBytes(summary?.total_bytes_received ?? null, i18n.lang)} ${t('dash.down')}`}
		>
			{#snippet icon()}<ArrowUp class="h-5 w-5" />{/snippet}
		</StatCard>
		<StatCard
			label={t('dash.todayTraffic')}
			value={formatBytes(todayTraffic, i18n.lang)}
			sub={`${formatBytes(summary?.today_bytes_sent ?? null, i18n.lang)} ${t('dash.up')} / ${formatBytes(summary?.today_bytes_received ?? null, i18n.lang)} ${t('dash.down')}`}
			tone="accent"
		>
			{#snippet icon()}<ArrowDown class="h-5 w-5" />{/snippet}
		</StatCard>
		<StatCard
			label={t('dash.activeKeys')}
			value={String(summary?.enabled_keys ?? '—')}
			sub={`${t('dash.totalKeys')}: ${summary?.total_keys ?? '—'}`}
			tone="ok"
		>
			{#snippet icon()}<KeyRound class="h-5 w-5" />{/snippet}
		</StatCard>
		<StatCard
			label={t('dash.overQuota')}
			value={String(summary?.over_quota_keys ?? '—')}
			tone={summary && summary.over_quota_keys > 0 ? 'warn' : 'muted'}
		>
			{#snippet icon()}<AlertTriangle class="h-5 w-5" />{/snippet}
		</StatCard>
		<StatCard
			label={t('dash.nodesOnline')}
			value={`${summary?.online_nodes ?? '—'} / ${summary?.total_nodes ?? '—'}`}
			tone="accent"
		>
			{#snippet icon()}<Server class="h-5 w-5" />{/snippet}
		</StatCard>
	</div>

	<!-- Chart + system load -->
	<div class="grid grid-cols-1 gap-4 lg:grid-cols-3">
		<div class="lg:col-span-2">
			<Card title={t('dash.usageTitle')} hint={t('dash.usageHint')}>
				{#snippet actions()}
					<div class="flex gap-1">
						{#each DAYS_OPTIONS as d (d)}
							<button
								type="button"
								class="rounded-md px-2 py-1 text-xs font-medium transition-colors {usageDays === d
									? 'bg-[var(--of-accent)] text-white'
									: 'bg-[var(--of-raise)] text-[var(--of-muted)] hover:text-[var(--of-ink)]'}"
								onclick={() => (usageDays = d)}
							>
								{d}
							</button>
						{/each}
					</div>
				{/snippet}
				<UsageChart data={chartData} lang={i18n.lang} />
			</Card>
		</div>
		<div>
			<Card title={t('dash.serverLoad')}>
				<SystemLoadCard system={system} />
			</Card>
		</div>
	</div>

	<!-- Nodes -->
	<Card title={t('dash.nodesTitle')}>
		<div class="overflow-x-auto">
			<table class="of-table of-stack">
				<thead>
					<tr>
						<th>{t('nodes.colName')}</th>
						<th>{t('nodes.colStatus')}</th>
						<th class="num">{t('nodes.colKeys')}</th>
						<th>{t('nodes.colHeartbeat')}</th>
					</tr>
				</thead>
				<tbody>
					{#each nodes as node (node.ID)}
						{@const since = node.LastHeartbeatAt ? timeAgoMs(node.LastHeartbeatAt) : null}
						{@const online = since !== null && since <= ONLINE_WINDOW_MS}
						<tr>
							<td class="of-title">
								<div class="flex items-center gap-2">
									<Users class="h-4 w-4 shrink-0 text-[var(--of-muted)]" />
									<span class="font-medium text-[var(--of-ink)]">{node.Name}</span>
								</div>
							</td>
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
							<td class="text-[var(--of-muted)]" data-label={t('nodes.colHeartbeat')}>{formatDate(node.LastHeartbeatAt, i18n.lang)}</td>
						</tr>
					{:else}
						<tr>
							<td colspan="4" class="text-center text-[var(--of-muted)]">{t('nodes.empty')}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</Card>
</div>