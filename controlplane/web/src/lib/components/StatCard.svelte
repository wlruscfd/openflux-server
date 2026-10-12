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

<div class="of-card px-4 py-4 sm:px-5 sm:py-5">
	<div class="flex items-center justify-between gap-2">
		<div class="min-w-0">
			<p class="text-[10.5px] leading-tight font-semibold tracking-wide text-[var(--of-muted)] uppercase">{label}</p>
			<p class="mt-2 text-2xl leading-none font-semibold tracking-tight break-words text-[var(--of-ink)] tabular-nums sm:text-[1.7rem]">{value}</p>
			{#if sub}
				<p class="mt-1.5 text-xs leading-snug text-[var(--of-muted)]">{sub}</p>
			{/if}
		</div>
		{#if icon}
			<span class="hidden shrink-0 self-start rounded-xl border border-[var(--of-line)] bg-[var(--of-raise)] p-2.5 sm:block {accents[tone]}">{@render icon()}</span>
		{/if}
	</div>
</div>