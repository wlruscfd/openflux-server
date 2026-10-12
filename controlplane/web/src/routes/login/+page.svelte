<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { KeyRound } from 'lucide-svelte';
	import { t } from '$lib/i18n';
	import { auth } from '$lib/state/auth.svelte';
	import { api, ApiError } from '$lib/api';

	let token = $state('');
	let busy = $state(false);
	let error = $state(false);

	async function submit() {
		if (busy || !token.trim()) return;
		busy = true;
		error = false;
		const trimmed = token.trim();
		auth.set(trimmed);
		try {
			await api.listNodes();
			await goto(`${base}/`);
		} catch (e) {
			auth.clear();
			if (e instanceof ApiError && e.status === 401) {
				error = true;
			} else {
				error = true;
			}
		} finally {
			busy = false;
		}
	}
</script>

<div class="flex min-h-screen items-center justify-center bg-[var(--of-bg)] px-4">
	<div class="w-full max-w-sm">
		<div class="mb-6 flex flex-col items-center gap-2 text-center">
			<span class="of-logo of-logo-lg">{t('app.name')}</span>
			<h1 class="text-lg font-semibold text-[var(--of-ink)]">{t('login.title')}</h1>
		</div>

		<form
			class="of-card p-6"
			onsubmit={(e) => {
				e.preventDefault();
				submit();
			}}
		>
			<label class="block" for="admin-token">
				<span class="mb-1.5 block text-xs font-medium text-[var(--of-muted)]">{t('login.tokenLabel')}</span>
				<div class="relative">
					<KeyRound class="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-[var(--of-muted)]" />
					<input
						id="admin-token"
						type="password"
						autocomplete="current-password"
						placeholder={t('login.tokenPlaceholder')}
						class="input !pl-9 font-mono"
						bind:value={token}
						disabled={busy}
					/>
				</div>
			</label>

			{#if error}
				<p class="mt-2 text-xs text-[var(--of-danger)]">{t('login.error')}</p>
			{/if}

			<button
				type="submit"
				class="of-btn-primary mt-5 w-full rounded-[0.7rem] py-3 text-sm font-semibold transition-all disabled:opacity-50"
				disabled={busy || !token.trim()}
			>
				{busy ? t('generic.loading') : t('login.submit')}
			</button>
		</form>

		<p class="mt-4 text-center text-xs text-[var(--of-muted)]">{t('login.subtitle')}</p>
	</div>
</div>