package outbound

var carrierHostTable = map[string][]string{
	"yandex": {
		"docs.yandex.ru", "disk.yandex.ru", "yandex.ru",
	},
	"boards": {
		"boards.yandex.ru", "socket33.boards.yandex.ru",
	},
	"telemost": {
		"cloud-api.yandex.ru",
	},
	"mailru": {
		"cloud.mail.ru", "mail.ru",
	},
	"mts": {
		"my.mts-link.ru", "wsboard.mts-link.ru", "wsboard2.mts-link.ru", "mts-link.ru",
	},
	"cupsonline": {
		"cups.online", "interview.cups.online",
	},
	"avito": {
		"code.avito.ru", "avito.ru",
	},
	"oneme": {
		"ws-api.oneme.ru", "oneme.ru",
	},
}

func (r *Router) LoadCarrierHosts() {
	for carrierType, hosts := range carrierHostTable {
		for _, host := range hosts {
			r.RegisterHost(carrierType, host)
		}
	}
}
