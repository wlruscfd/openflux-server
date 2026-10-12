<script lang="ts">
	import { t } from '$lib/i18n';
	import { formatBytes, formatDateShort } from '$lib/format';

	interface Point {
		day: string;
		sent: number;
		recv: number;
	}

	interface Props {
		data: Point[];
		lang?: 'ru' | 'en';
		height?: number;
	}

	let { data, lang = 'ru', height = 210 }: Props = $props();

	const PAD_L = 52;
	const PAD_R = 6;
	const PAD_T = 10;
	const PAD_B = 24;

	let width = $state(0);
	let hover = $state<number | null>(null);

	const innerW = $derived(Math.max(0, width - PAD_L - PAD_R));
	const innerH = $derived(height - PAD_T - PAD_B);
	const max = $derived(Math.max(...data.map((d) => d.sent + d.recv), 1));

	const niceMax = $derived(
		((): number => {
			const exp = Math.pow(1024, Math.floor(Math.log(max) / Math.log(1024)));
			const steps = [1, 2, 4, 5, 8, 10, 16, 20, 32, 40, 50, 64, 80, 100, 128, 200, 256, 400, 500, 512, 800, 1000, 1024];
			const n = max / exp;
			return (steps.find((s) => s >= n) ?? Math.ceil(n)) * exp;
		})()
	);
	const ticks = $derived([0, 0.25, 0.5, 0.75, 1].map((f) => f * niceMax));

	function y(value: number): number {
		return PAD_T + innerH - (value / niceMax) * innerH;
	}

	const slot = $derived(data.length > 0 ? innerW / data.length : 1);
	const barW = $derived(Math.max(2, Math.min(26, slot * 0.68)));
	const labelEvery = $derived(Math.max(1, Math.ceil(data.length / Math.max(2, Math.floor(innerW / 54)))));

	function onMove(e: MouseEvent | PointerEvent) {
		const rect = (e.currentTarget as SVGElement).getBoundingClientRect();
		const i = Math.floor((e.clientX - rect.left - PAD_L) / slot);
		hover = i >= 0 && i < data.length ? i : null;
	}

	const tip = $derived(hover !== null ? data[hover] : null);
	const tipLeft = $derived(hover !== null ? PAD_L + hover * slot + slot / 2 : 0);
</script>

<div class="w-full">
	<div class="mb-2 flex items-center gap-4 text-xs text-[var(--of-muted)]">
		<span class="inline-flex items-center gap-1.5"><span class="h-2.5 w-2.5 rounded-sm bg-[var(--of-accent)]"></span>{t('dash.down')}</span>
		<span class="inline-flex items-center gap-1.5"><span class="h-2.5 w-2.5 rounded-sm bg-[var(--of-warn)]"></span>{t('dash.up')}</span>
	</div>

	<div class="relative w-full" style="height: {height}px" bind:clientWidth={width}>
		{#if data.length === 0 || width === 0}
			<p class="flex h-full items-center justify-center text-sm text-[var(--of-muted)]">{t('generic.loading')}</p>
		{:else}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<svg
				width={width}
				height={height}
				class="block touch-pan-y select-none"
				onpointermove={onMove}
				onpointerleave={() => (hover = null)}
			>
				{#each ticks as tick (tick)}
					<line x1={PAD_L} x2={width - PAD_R} y1={y(tick)} y2={y(tick)} stroke="var(--of-line)" stroke-width="1" stroke-dasharray={tick === 0 ? undefined : '3 4'} />
					<text x={PAD_L - 8} y={y(tick) + 3.5} text-anchor="end" fill="var(--of-muted)" font-size="10.5" class="tabular-nums">{tick === 0 ? '0' : formatBytes(tick, lang)}</text>
				{/each}

				{#each data as d, i (d.day)}
					{@const x = PAD_L + i * slot + (slot - barW) / 2}
					{@const yRecv = y(d.recv)}
					{@const yTotal = y(d.sent + d.recv)}
					<g opacity={hover === null || hover === i ? 1 : 0.55}>
						<rect {x} y={yRecv} width={barW} height={Math.max(0, y(0) - yRecv)} fill="var(--of-accent)" />
						<rect {x} y={yTotal} width={barW} height={Math.max(0, yRecv - yTotal)} rx="2" fill="var(--of-warn)" />
					</g>
				{/each}

				{#each data as d, i (d.day)}
					{#if i % labelEvery === 0}
						<text x={PAD_L + i * slot + slot / 2} y={height - 7} text-anchor="middle" fill="var(--of-muted)" font-size="10.5">{d.day.slice(5)}</text>
					{/if}
				{/each}
			</svg>

			{#if tip}
				<div
					class="pointer-events-none absolute top-0 z-10 rounded-lg border border-[var(--of-line-strong)] bg-[var(--of-panel)] px-2.5 py-1.5 text-xs shadow-lg"
					style="left: {Math.min(Math.max(tipLeft, 70), width - 70)}px; transform: translateX(-50%)"
				>
					<p class="font-medium text-[var(--of-ink)]">{formatDateShort(tip.day, lang)}</p>
					<p class="tabular-nums text-[var(--of-muted)]">
						<span class="text-[var(--of-accent)]">↓ {formatBytes(tip.recv, lang)}</span> ·
						<span class="text-[var(--of-warn)]">↑ {formatBytes(tip.sent, lang)}</span>
					</p>
				</div>
			{/if}
		{/if}
	</div>
</div>
