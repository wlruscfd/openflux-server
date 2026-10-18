<script lang="ts">
	import type { Snippet } from 'svelte';
	import { page } from '$app/stores';
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { LayoutDashboard, KeyRound, Server, PlugZap, Sun, Moon, LogOut, Network } from 'lucide-svelte';
	import { t } from '$lib/i18n';
	import { i18n } from '$lib/i18n';
	import { LANGS, type Lang } from '$lib/i18n';
	import { themeStore } from '$lib/state/theme.svelte';
	import { auth } from '$lib/state/auth.svelte';
	import { confirmDialog } from '$lib/state/confirm.svelte';

	interface Props {
		children: Snippet;
	}

	let { children }: Props = $props();

	const homeHref = `${base}/`;
	const items = [
		{ href: homeHref, key: 'nav.dashboard', Icon: LayoutDashboard, match: (p: string) => p === homeHref.replace(/\/$/, '') || p === homeHref },
		{ href: `${base}/keys`, key: 'nav.keys', Icon: KeyRound, match: (p: string) => p.startsWith(`${base}/keys`) },
		{ href: `${base}/nodes`, key: 'nav.nodes', Icon: Server, match: (p: string) => p.startsWith(`${base}/nodes`) },
		{
			href: `${base}/proxy-keys`,
			key: 'nav.proxyKeys',
			Icon: Network,
			match: (p: string) => p.startsWith(`${base}/proxy-keys`)
		},
		{ href: `${base}/ingest`, key: 'nav.ingest', Icon: PlugZap, match: (p: string) => p.startsWith(`${base}/ingest`) }
	];

	async function logout() {
		confirmDialog.ask({
			title: t('nav.logoutConfirm'),
			confirmLabel: t('nav.logout'),
			tone: 'danger',
			onConfirm: () => {
				auth.clear();
				goto(`${base}/login`);
			}
		});
	}
</script>

{#snippet controls()}
	<div class="flex overflow-hidden rounded-lg border border-[var(--of-line-strong)]">
		{#each LANGS as l (l)}
			<button
				type="button"
				class="px-2.5 py-1.5 text-xs font-semibold transition-colors {i18n.lang === l
					? 'bg-[var(--of-accent)] text-white'
					: 'text-[var(--of-muted)] hover:text-[var(--of-ink)]'}"
				onclick={() => i18n.setLang(l as Lang)}
			>
				{l.toUpperCase()}
			</button>
		{/each}
	</div>
	<button
		type="button"
		class="of-iconbtn"
		onclick={themeStore.toggle}
		title={t('theme.toggle')}
		aria-label={t('theme.toggle')}
	>
		{#if themeStore.current === 'dark'}
			<Sun class="h-[18px] w-[18px]" />
		{:else}
			<Moon class="h-[18px] w-[18px]" />
		{/if}
	</button>
	<button
		type="button"
		class="of-iconbtn hover:!text-[var(--of-danger)]"
		onclick={logout}
		title={t('nav.logout')}
		aria-label={t('nav.logout')}
	>
		<LogOut class="h-[18px] w-[18px]" />
	</button>
{/snippet}

<div class="min-h-screen lg:grid lg:grid-cols-[250px_minmax(0,1fr)]">
	<aside
		class="sticky top-0 hidden h-screen flex-col border-r border-[var(--of-line)] bg-[var(--of-panel)]/70 px-5 py-6 backdrop-blur lg:flex"
	>
		<a href={homeHref} class="mb-8 px-1"><span class="of-logo">{t('app.name')}</span></a>

		<nav class="flex flex-col gap-1.5 pl-3" aria-label="Primary">
			{#each items as item (item.key)}
				<a href={item.href} class="of-navlink" aria-current={item.match($page.url.pathname) ? 'page' : undefined}>
					<item.Icon class="h-[18px] w-[18px]" />
					{t(item.key)}
				</a>
			{/each}
		</nav>

		<div class="mt-auto space-y-3">
			<div class="flex items-center justify-between gap-1">{@render controls()}</div>
			<p class="px-1 text-xs text-[var(--of-muted)]">{t('app.name')} · {t('app.panel')}</p>
		</div>
	</aside>

	<div class="flex min-h-screen min-w-0 flex-col">
		<header class="sticky top-0 z-40 border-b border-[var(--of-line)] bg-[var(--of-panel)]/80 backdrop-blur lg:hidden">
			<div class="flex h-14 items-center gap-3 px-4">
				<a href={homeHref}><span class="of-logo">{t('app.name')}</span></a>
				<div class="ml-auto flex items-center gap-1.5">{@render controls()}</div>
			</div>
		</header>

		<main class="of-page-pad mx-auto w-full max-w-6xl flex-1 px-4 pt-5 sm:px-6 sm:pt-7 lg:px-10 lg:pt-10">
			{@render children()}
		</main>
	</div>

	<nav
		class="of-tabbar fixed inset-x-0 bottom-0 z-40 border-t border-[var(--of-line)] bg-[var(--of-panel)]/92 backdrop-blur lg:hidden"
		aria-label="Primary"
	>
		<div class="mx-auto grid max-w-lg grid-cols-5">
			{#each items as item (item.key)}
				{@const active = item.match($page.url.pathname)}
				<a
					href={item.href}
					aria-current={active ? 'page' : undefined}
					class="flex flex-col items-center gap-0.5 px-1 pt-2 pb-2 text-[11px] font-medium transition-colors {active
						? 'text-[var(--of-accent)]'
						: 'text-[var(--of-muted)]'}"
				>
					<span class="rounded-full px-4 py-1 transition-colors {active ? 'bg-[var(--of-accent)]/15' : ''}">
						<item.Icon class="h-5 w-5" />
					</span>
					<span class="max-w-full truncate">{t(item.key)}</span>
				</a>
			{/each}
		</div>
	</nav>
</div>
