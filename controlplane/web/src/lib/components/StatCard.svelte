<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		label: string;
		value: string;
		sub?: string;
		tone?: 'muted' | 'accent' | 'ok' | 'warn' | 'danger';
		icon?: Snippet;
	}

	let { label, value, sub, tone = 'muted', icon }: Props = $props();

	const accents = {
		muted: 'text-[var(--of-muted)]',
		accent: 'text-[var(--of-accent)]',
		ok: 'text-[var(--of-ok)]',
		warn: 'text-[var(--of-warn)]',
		danger: 'text-[var(--of-danger)]'
	} as const;
</script>

<div class="rounded-xl border border-[var(--of-line)] bg-[var(--of-panel)] px-4 py-3.5 sm:px-5 sm:py-4">
	<div class="flex items-center justify-between gap-2">
		<div class="min-w-0">
			<p class="text-xs font-medium text-[var(--of-muted)]">{label}</p>
			<p class="mt-1 truncate text-2xl leading-none font-semibold text-[var(--of-ink)] tabular-nums">{value}</p>
			{#if sub}
				<p class="mt-1.5 line-clamp-2 text-xs leading-snug text-[var(--of-muted)]">{sub}</p>
			{/if}
		</div>
		{#if icon}
			<span class="shrink-0 self-start rounded-lg bg-[var(--of-raise)] p-2 {accents[tone]}">{@render icon()}</span>
		{/if}
	</div>
</div>