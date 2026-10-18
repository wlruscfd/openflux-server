package yandex

import "testing"

// A share link without the trailing slash is answered with a 301 to the same board with it. The
// carrier used to treat any non-captcha redirect as fatal, which is what left every such board
// stuck on "boards: unexpected redirect".
func TestSameHostAcceptsBoardsRelativeAndAbsolute(t *testing.T) {
	cases := map[string]bool{
		"/whiteboard/?hash=abc":               true,
		"https://boards.yandex.ru/x":          true,
		"https://BOARDS.YANDEX.RU/x":          true,
		"//boards.yandex.ru/x":                true,
		"https://showcaptchafast.yandex.ru/x": false,
		"https://yandex.ru/x":                 false,
		"https://evil.example/x":              false,
	}
	for loc, want := range cases {
		if got := sameHost(loc, "boards.yandex.ru"); got != want {
			t.Fatalf("sameHost(%q) = %v, want %v", loc, got, want)
		}
	}
}
