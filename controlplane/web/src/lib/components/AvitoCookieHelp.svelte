<script lang="ts">
	import { t } from '$lib/i18n';
</script>

<details class="rounded-xl border border-[var(--of-line)] bg-[var(--of-raise)] px-3 py-2 text-xs text-[var(--of-muted)]">
	<summary class="cursor-pointer font-medium text-[var(--of-ink)]">{t('keys.avitoHowTitle')}</summary>
	<ol class="mt-2 list-decimal space-y-1.5 pl-5">
		<li>{t('keys.avitoHow1')}</li>
		<li>{t('keys.avitoHow2')}</li>
		<li>{t('keys.avitoHow3')}</li>
		<li>{t('keys.avitoHow4')}</li>
		<li>{t('keys.avitoHow5')}</li>
	</ol>
	<p class="mt-2">{t('keys.avitoHowWarn')}</p>
</details>
