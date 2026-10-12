<script lang="ts">
	import type { Snippet } from 'svelte';
	import type { HTMLButtonAttributes } from 'svelte/elements';

	interface Props extends HTMLButtonAttributes {
		variant?: 'primary' | 'secondary' | 'danger' | 'ghost';
		size?: 'sm' | 'md';
		children: Snippet;
	}

	let {
		variant = 'secondary',
		size = 'md',
		type = 'button',
		disabled = false,
		children,
		class: klass = '',
		...rest
	}: Props = $props();

	const variants = {
		primary: 'of-btn-primary',
		secondary: 'of-btn-secondary',
		danger: 'bg-[var(--of-danger)] text-white hover:opacity-90',
		ghost: 'text-[var(--of-muted)] hover:text-[var(--of-ink)] hover:bg-[var(--of-raise)]'
	} as const;

	const sizes = {
		sm: 'px-3 py-1.5 text-xs',
		md: 'px-4 py-2.5 text-sm'
	} as const;
</script>

<!-- svelte-ignore a11y_click_events_have_key_events -->
<button
	{type}
	{disabled}
	class="inline-flex items-center justify-center gap-1.5 rounded-[0.7rem] font-semibold transition-all select-none disabled:pointer-events-none disabled:opacity-50 whitespace-nowrap {variants[variant]} {sizes[size]} {klass}"
	{...rest}
>
	{@render children()}
</button>