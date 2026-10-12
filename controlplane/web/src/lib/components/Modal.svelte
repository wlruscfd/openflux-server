<script lang="ts">
	import type { Snippet } from 'svelte';
	import { X } from 'lucide-svelte';

	interface Props {
		open: boolean;
		title: string;
		hint?: string;
		footer?: Snippet;
		children: Snippet;
		onClose?: () => void;
		wide?: boolean;
		showClose?: boolean;
	}

	let { open, title, hint, footer, children, onClose, wide = false, showClose = true }: Props = $props();

	$effect(() => {
		if (!open) return;
		const prev = document.body.style.overflow;
		document.body.style.overflow = 'hidden';
		const onKey = (e: KeyboardEvent) => {
			if (e.key === 'Escape') onClose?.();
		};
		window.addEventListener('keydown', onKey);
		return () => {
			document.body.style.overflow = prev;
			window.removeEventListener('keydown', onKey);
		};
	});
</script>

{#if open}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<div
		role="presentation"
		class="fixed inset-0 z-50 flex items-end justify-center overflow-y-auto bg-black/50 backdrop-blur-sm sm:items-center sm:p-4"
		onclick={(e) => {
			if (e.target === e.currentTarget) onClose?.();
		}}
	>
		<div
			role="dialog"
			aria-modal="true"
			class="max-h-[92vh] sm:my-auto w-full overflow-y-auto rounded-t-3xl border border-[var(--of-line-strong)] bg-[var(--of-panel)] shadow-2xl sm:rounded-2xl {wide
				? 'max-w-2xl'
				: 'max-w-md'}"
		>
			<header class="flex items-start justify-between gap-3 border-b border-[var(--of-line)] px-5 py-4">
				<div>
					<h2 class="text-base font-semibold text-[var(--of-ink)]">{title}</h2>
					{#if hint}
						<p class="mt-0.5 text-xs text-[var(--of-muted)]">{hint}</p>
					{/if}
				</div>
				{#if showClose}
					<button
						type="button"
						class="rounded-lg p-1.5 text-[var(--of-muted)] transition-colors hover:bg-[var(--of-raise)] hover:text-[var(--of-ink)]"
						onclick={onClose}
						aria-label="Close"
					>
						<X class="h-4 w-4" />
					</button>
				{/if}
			</header>

			<div class="px-5 py-4">{@render children()}</div>

			{#if footer}
				<footer class="flex flex-wrap justify-end gap-2 border-t border-[var(--of-line)] px-5 py-4 pb-[max(1rem,env(safe-area-inset-bottom))]">
					{@render footer()}
				</footer>
			{/if}
		</div>
	</div>
{/if}