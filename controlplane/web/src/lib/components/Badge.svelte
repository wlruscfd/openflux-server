<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		tone?: 'ok' | 'warn' | 'danger' | 'muted' | 'accent';
		dot?: boolean;
		children: Snippet;
	}

	let { tone = 'muted', dot = false, children }: Props = $props();

	const tones = {
		ok: ['bg-[var(--of-ok)]', 'bg-[var(--of-ok)]/15 text-[var(--of-ok)]'],
		warn: ['bg-[var(--of-warn)]', 'bg-[var(--of-warn)]/15 text-[var(--of-warn)]'],
		danger: ['bg-[var(--of-danger)]', 'bg-[var(--of-danger)]/15 text-[var(--of-danger)]'],
		muted: ['bg-[var(--of-muted)]', 'bg-[var(--of-raise)] text-[var(--of-muted)]'],
		accent: ['bg-[var(--of-accent)]', 'bg-[var(--of-accent)]/15 text-[var(--of-accent)]']
	} as const;
</script>

<span class="inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium whitespace-nowrap {tones[tone][1]}">
	{#if dot}
		<span class="h-1.5 w-1.5 rounded-full {tones[tone][0]}" style="box-shadow: 0 0 8px currentColor"></span>
	{/if}
	{@render children()}
</span>