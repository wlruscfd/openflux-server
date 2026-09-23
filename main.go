package main

import (
	"context"
	"flag"
	"fmt"
	_ "github.com/wlynxg/anet"
	"log"
	"os"
	"os/signal"
	rtdebug "runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"universal-bypass-tool/nodeagent"
	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/cupsonline"
	"universal-bypass-tool/transport/mailru"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

var (
	globalDocUrl string
	maxToken     string
	maxUid       string
)

func main() {
	fmt.Print("written by p1neappleXpress\n")

	exitNode := flag.Bool("exit-node", false, "Run as exit node (needs root)")
	client := flag.Bool("client", false, "Run as client")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	socksAddr := flag.String("socks5", ":1080", "SOCKS5 address")
	transportType := flag.String("transport", "yandex", "Transport type (yandex, volga, oneme, yandex_multistream, cupsonline, mailru)")
	managed := flag.Bool("managed", false, "Exit node only: fetch active keys from a controlplane instance instead of a single --url")
	controlURL := flag.String("control-url", "", "Managed mode: base URL of the openflux-control service")
	nodeToken := flag.String("node-token", "", "Managed mode: this node's bearer token from controlplane")
	mode := flag.String("mode", "raw", "Exit node only: 'raw' (default, needs root; raw socket + gvisor NAT, forwards any IP protocol) or 'proxy' (no root, no raw socket; TCP only - see README)")
	localIP := flag.String("local-ip", "", "Raw mode only: exit node egress IP, so the RST-drop iptables rule can be scoped with -s instead of host-wide")
	portRangeSize := flag.Int("port-range-size", nodeagent.DefaultPortRangeSize, "Managed raw mode only: outbound ports reserved per concurrent key - lower fits more keys on this node, higher tolerates a single key opening more simultaneous connections at once (e.g. Telegram loading media) before new ones start failing")
	codec := flag.String("codec", "legacy", "Wire codec for --transport volga/oneme/cupsonline/mailru: 'legacy' (default, per-packet LZ4 - unchanged) or 'batched' (coalesce bursts into one zstd-compressed message per transport send; both ends must agree - see README). Ignored for yandex/yandex_multistream, which auto-negotiate their own whole-batch zstd format with the peer - see README.")
	captchaSolveMode := flag.String("captcha-solve-mode", "off", "Exit node only (yandex/yandex_multistream): how to react to a Yandex CAPTCHA with no user present to solve it - 'off' (default; just wait out the normal cooldown-and-retry - browser_ua.go's User-Agent choice already avoids triggering most CAPTCHAs) or 'headless_browser' (also try a shared headless Chrome/Chromium automatically, needs it on PATH - see README)")
	flag.StringVar(&globalDocUrl, "url", "http://#", "Document URL. If u use Yandex.Docs transport")
	docUrls := flag.String("urls", "", "Comma-separated doc URLs for --transport yandex_multistream (2+ required, same list on both ends)")
	flag.StringVar(&maxToken, "maxToken", "", "MAX call user id. If u use MAX transport")
	flag.StringVar(&maxUid, "maxUid", "", "MAX Web token. If u use MAX transport")
	flag.Parse()

	if !*exitNode && !*client {
		flag.Usage()
		os.Exit(1)
	}

	if *debug {
		utils.EnableDebug()
		utils.SetVerbose(true)
	}

	exitMode, modeErr := tunnel.ParseExitMode(*mode)
	if modeErr != nil {
		log.Fatalf("%v", modeErr)
	}
	if *localIP != "" {
		tunnel.SetLocalIP(*localIP)
	}
	if *exitNode {
		// Keep the heap tight (GOMEMLIMIT) for small-VPS deployments; avoid --debug in production since per-packet logging is the main allocation source.
		rtdebug.SetGCPercent(20)
	}

	if *managed {
		if !*exitNode {
			log.Fatalf("--managed is only valid together with --exit-node")
		}
		if *controlURL == "" || *nodeToken == "" {
			log.Fatalf("--managed requires --control-url and --node-token")
		}

		log.Printf("=== Universal Bypass Tool ===")
		log.Printf("Mode: EXIT NODE (managed, control=%s, exit-mode=%s)", *controlURL, exitMode)

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		cfg := nodeagent.DefaultConfig(*controlURL, *nodeToken)
		cfg.ExitMode = exitMode
		cfg.PortRangeSize = *portRangeSize
		cfg.CaptchaSolveMode = parseCaptchaSolveMode(*captchaSolveMode)
		orch := nodeagent.NewOrchestrator(cfg)
		orch.Run(ctx)
		return
	}

	log.Printf("=== Universal Bypass Tool ===")
	log.Printf("Mode: %s", map[bool]string{true: "EXIT NODE", false: "CLIENT"}[*exitNode])
	log.Printf("Transport: %s", *transportType)

	config := transport.DefaultConfig()
	var trans transport.Transport

	wrapCodec := func(inner transport.Transport) transport.Transport {
		wrapped, err := transport.WrapCodec(inner, *codec)
		if err != nil {
			log.Fatalf("%v", err)
		}
		return wrapped
	}

	// selfCompressingYandex manages its own per-batch compression and ignores --codec; --codec still applies to volga/oneme.
	selfCompressingYandex := func(url string) transport.Transport {
		yd := yandex.NewYandexDocsTransport(url, config)
		yd.EnableSelfCompression()
		if *exitNode {
			yd.SetCaptchaSolveMode(parseCaptchaSolveMode(*captchaSolveMode))
		}
		return yd
	}

	switch *transportType {
	case "yandex":
		trans = selfCompressingYandex(globalDocUrl)
	case "volga":
		trans = wrapCodec(yandex.NewYandexVolgaTransport(globalDocUrl, config))
	case "oneme":
		uidint, _ := strconv.ParseInt(maxUid, 10, 64)
		trans = wrapCodec(oneme.NewOneMeTransport(*exitNode, maxToken, uidint, config))
	case "cupsonline":
		trans = wrapCodec(cupsonline.NewCupsonlineTransport(globalDocUrl, config, !*exitNode))
	case "mailru":
		trans = wrapCodec(mailru.NewMailruDocsTransport(globalDocUrl, config))
	case "yandex_multistream":
		urls := strings.Split(*docUrls, ",")
		if len(urls) < 2 {
			log.Fatalf("--transport yandex_multistream requires --urls with 2+ comma-separated doc URLs")
		}
		streams := make([]transport.Transport, len(urls))
		for i, url := range urls {
			streams[i] = selfCompressingYandex(strings.TrimSpace(url))
		}
		trans = transport.NewMultiStreamTransport(streams)
	default:
		log.Fatalf("Unknown transport type: %s", *transportType)
	}

	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	tun := tunnel.NewTCPTunnelMode(trans, *exitNode, exitMode)
	// Read back the tunnel's actual mode: raw mode silently falls back to proxy mode if raw-socket creation fails.
	exitMode = tun.ExitMode()

	if *exitNode {
		if exitMode == tunnel.ExitModeProxy {
			log.Printf("Running as EXIT NODE (proxy mode - no root, no raw socket, TCP only)")
		} else {
			log.Printf("Running as EXIT NODE (raw mode, needs root for raw socket)")
			if *localIP != "" {
				// Scoped: only drop kernel RSTs from the tunnel's own egress IP, leaving other host services untouched.
				log.Printf("! Run: sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -s %s -j DROP", *localIP)
			} else {
				log.Printf("! Kernel RSTs would tear down tunnel connections. Prefer a scoped rule:")
				log.Printf("!   assign a dedicated alias IP, run with --local-ip <ip>, then:")
				log.Printf("!   sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -s <ip> -j DROP")
				log.Printf("! Host-wide fallback (drops ALL outbound RST; makes closed ports look filtered):")
				log.Printf("!   sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP")
			}
		}
		select {}
	} else {
		log.Printf("Running as CLIENT (SOCKS5 on %s)", *socksAddr)
		socks5Server := socks5.NewSOCKS5Server(*socksAddr, tun)
		log.Fatal(socks5Server.Start())
	}
}

func parseCaptchaSolveMode(s string) yandex.CaptchaSolveMode {
	if s == "headless_browser" {
		return yandex.CaptchaSolveModeHeadlessBrowser
	}
	return yandex.CaptchaSolveModeOff
}
