<script lang="ts">
	import type { Snippet } from 'svelte';
	import { page } from '$app/stores';
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { LayoutDashboard, KeyRound, Server, PlugZap, Sun, Moon, LogOut } from 'lucide-svelte';
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

<div class="flex min-h-screen flex-col">
	<header
		class="sticky top-0 z-40 border-b border-[var(--of-line)] bg-[var(--of-panel)]/85 backdrop-blur"
	>
		<div class="mx-auto flex h-14 w-full max-w-6xl items-center gap-3 px-4">
			<a href={homeHref} class="flex items-center gap-2">
				<span class="of-logo">{t('app.name')}</span>
			</a>

			<nav class="ml-4 hidden items-center gap-1 md:flex">
				{#each items as item (item.key)}
					<a
						href={item.href}
						aria-current={item.match($page.url.pathname) ? 'page' : undefined}
						class="flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-sm font-medium whitespace-nowrap transition-colors {item.match($page.url.pathname)
							? 'bg-[var(--of-raise)] text-[var(--of-accent)]'
							: 'text-[var(--of-muted)] hover:bg-[var(--of-raise)] hover:text-[var(--of-ink)]'}"
					>
						<item.Icon class="h-4 w-4" />
						{t(item.key)}
					</a>
				{/each}
			</nav>

			<div class="ml-auto flex items-center gap-1.5">
				<div class="flex overflow-hidden rounded-lg border border-[var(--of-line)]">
					{#each LANGS as l (l)}
						<button
							type="button"
							class="px-2 py-1.5 text-xs font-medium transition-colors {i18n.lang === l
								? 'bg-[var(--of-accent)] text-white'
								: 'bg-[var(--of-panel)] text-[var(--of-muted)] hover:text-[var(--of-ink)]'}"
							onclick={() => i18n.setLang(l as Lang)}
						>
							{l.toUpperCase()}
						</button>
					{/each}
				</div>

				<button
					type="button"
					class="rounded-lg p-2 text-[var(--of-muted)] transition-colors hover:bg-[var(--of-raise)] hover:text-[var(--of-ink)]"
					onclick={themeStore.toggle}
					title={t('theme.toggle')}
					aria-label={t('theme.toggle')}
				>
					{#if themeStore.current === 'dark'}
						<Sun class="h-4 w-4" />
					{:else}
						<Moon class="h-4 w-4" />
					{/if}
				</button>

				<button
					type="button"
					class="rounded-lg p-2 text-[var(--of-muted)] transition-colors hover:bg-[var(--of-raise)] hover:text-[var(--of-danger)]"
					onclick={logout}
					title={t('nav.logout')}
					aria-label={t('nav.logout')}
				>
					<LogOut class="h-4 w-4" />
				</button>
			</div>
		</div>
	</header>

	<main class="of-page-pad mx-auto w-full max-w-6xl flex-1 px-4 pt-5 sm:pt-6">{@render children()}</main>

	<footer class="hidden border-t border-[var(--of-line)] py-4 md:block">
		<p class="mx-auto max-w-6xl px-4 text-xs text-[var(--of-muted)]">
			{t('app.name')} · {t('app.panel')}
		</p>
	</footer>

	<nav
		class="of-tabbar fixed inset-x-0 bottom-0 z-40 border-t border-[var(--of-line)] bg-[var(--of-panel)]/95 backdrop-blur md:hidden"
		aria-label="Primary"
	>
		<div class="mx-auto grid max-w-md grid-cols-4">
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
