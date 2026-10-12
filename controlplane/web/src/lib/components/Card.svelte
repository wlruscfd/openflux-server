<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		title?: string;
		hint?: string;
		actions?: Snippet;
		children: Snippet;
		flush?: boolean;
		class?: string;
	}

	let { title, hint, actions, children, flush = false, class: klass = '' }: Props = $props();
</script>

<section class="of-card {klass}">
	{#if title}
		<header class="flex flex-wrap items-center justify-between gap-2 px-4 pt-4 pb-1 sm:px-6 sm:pt-5">
			<div>
				<h2 class="text-[15px] font-semibold text-[var(--of-ink)]">{title}</h2>
				{#if hint}
					<p class="mt-0.5 text-xs text-[var(--of-muted)]">{hint}</p>
				{/if}
			</div>
			{#if actions}
				<div class="flex items-center gap-2">{@render actions()}</div>
			{/if}
		</header>
	{/if}
	<div class={flush ? '' : 'p-4 sm:p-6'}>
		{@render children()}
	</div>
</section>