// Bitrix24 video call transport: the tunnel rides the call's signaling
// event bus (sendMessage -> newMessage), which the server relays to every
// participant without inspecting. Both peers join the same conference as
// guests; the node keeps the call alive (it recreates one that has ended).
//
// The guest session (cookie jar of a browser that joined the conference
// once) is the key material: the page hands out the signed user/call tokens
// only to a registered guest session, everything after that is headless.

var UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36";
var EVENT = "Call::openflux";
var CHUNK_BYTES = 7168;

var Transport = {
  info: function () {
    return {
      name: "bitrix-call",
      version: "1.0.0",
      cookieDomain: "",
      mtu: 0,
      reliable: true,
      ordered: true,
      params: [
        { key: "url", label: "Conference URL", type: "url", required: true,
          description: "Bitrix24 video conference link (https://portal/video/xxx)" },
        { key: "tokens", label: "Guest tokens", type: "secret", required: false, group: "Session",
          description: "userToken|callToken of a registered guest - open the conference link in a browser, join once, and take both JWTs from the page source. Each side needs its own guest." },
        { key: "session", label: "Guest session cookies", type: "secret", required: false, group: "Session",
          description: "Cookie string of that same browser session; used to fetch fresh tokens when the tokens param is empty." },
        { key: "role", label: "Role", type: "select", options: ["client", "node"], default: "client", group: "Advanced",
          description: "node recreates the call when it has ended; managed nodes get this from the core automatically" },
        { key: "chunkMs", label: "Send interval, ms", type: "number", default: 20, min: 8, max: 200, advanced: true,
          description: "the provider drops bus traffic past ~4.6 Mbit/s per socket; 15ms is the lossless point" },
      ],
    };
  },

  open: async function (cfg) {
    this._cfg = cfg;
    this._portal = String(cfg.url || "").replace(/\/+$/, "");
    this._isExit = cfg.params.exit === true || cfg.params.exit === "true" || cfg.params.role === "node";
    this._chunkMs = Number(cfg.params.chunkMs) || 20;
    this._session = String(cfg.params.session || "").trim();
    this._sid = "";
    this._buf = [];
    this._bufLen = 0;
    this._pc = null;
    this._sock = null;
    this._closed = false;
    this._connect();
  },

  _connect: async function () {
    if (this._closed) return;
    setState("connecting");
    try {
      if (!this._toks) {
        if (this._cfg.params.tokens) {
          var parts = String(this._cfg.params.tokens).split("|");
          this._toks = {
            userToken: { raw: parts[0], payload: JSON.parse(text.decode(base64.decode(pad64(parts[0].split(".")[1])))) },
            callToken: { raw: parts[1], payload: JSON.parse(text.decode(base64.decode(pad64(parts[1].split(".")[1])))) },
          };
        } else {
          this._toks = await this._fetchTokens();
        }
      }
      var toks = this._toks;
      var joined = await this._join(toks);
      await this._socket(joined);
    } catch (e) {
      if (this._closed) return;
      var msg = String(e && e.message || e);
      if (msg.indexOf("not a registered guest") >= 0 || msg.indexOf("session cookies") >= 0) {
        setState("dead", msg);
        return;
      }
      setState("reconnecting", msg);
      var self = this;
      setTimeout(function () { self._connect(); }, 5000);
    }
  },

  _cookieHeader: function () {
    if (this._session) return this._session;
    var jar = cookieJar.get() || {};
    var parts = [];
    for (var k in jar) parts.push(k + "=" + jar[k]);
    return parts.join("; ");
  },

  _fetchTokens: async function () {
    var res = await http.fetch({
      url: this._portal,
      method: "GET",
      headers: { "Cookie": this._cookieHeader(), "User-Agent": UA },
    });
    if (res.status !== 200) throw new Error("conference page: http " + res.status);
    var toks = res.body.match(/eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g) || [];
    var userToken = null;
    var callToken = null;
    for (var i = 0; i < toks.length; i++) {
      var payload = JSON.parse(text.decode(base64.decode(pad64(toks[i].split(".")[1]))));
      if (payload.userId !== undefined && payload.userId !== null && userToken === null) userToken = { raw: toks[i], payload: payload };
      if (payload.portalId !== undefined && callToken === null) callToken = { raw: toks[i], payload: payload };
    }
    if (!userToken || !callToken) throw new Error("conference page carries no session tokens");
    if (!userToken.payload.userId) throw new Error("session cookies are not a registered guest: join the conference once in a browser and copy its cookies");
    var sessid = "";
    var sm = res.body.match(/"sessid":"([a-f0-9]{32})"/);
    if (sm) sessid = sm[1];
    setState("connecting", "page: user " + userToken.payload.userId);
    return { userToken: userToken, callToken: callToken, sessid: sessid };
  },

  _join: async function (toks) {
    var chatId = String(toks.callToken.payload.chatId || "");
    var callUuid = "";
    var mustCreate = this._isExit;
    for (var attempt = 0; attempt < 2; attempt++) {
      var res = await http.fetch({
        url: "https://slb.bitrix24.tech/v2/join?mustCreate=" + mustCreate,
        method: "POST",
        headers: { "Content-Type": "application/json", "chatId": chatId, "User-Agent": UA },
        body: JSON.stringify({
          userToken: toks.userToken.raw,
          isOneToOne: false,
          clientVersion: "1.0.0",
          clientPlatform: "web",
          callType: 2,
          roomType: 1,
          instanceId: "ofx-" + Math.random().toString(16).slice(2, 14) + Date.now().toString(16),
          callToken: toks.callToken.raw,
          provider: "Bitrix",
          isVideo: false,
          callUuid: callUuid,
        }),
      });
      var data = null;
      try { data = JSON.parse(res.body); } catch (e) {}
      if (data && data.result && data.result.mediaServerUrl) {
        this._chatId = chatId;
        setState("connecting", "joined room " + String(data.result.roomId).slice(0, 8));
        return data.result;
      }
      var errText = data && data.error ? (data.error.message || data.error.code) : ("http " + res.status);
      if (this._isExit && attempt === 0) {
        mustCreate = true;
        continue;
      }
      throw new Error("join: " + errText);
    }
    throw new Error("join: unreachable");
  },

  _socket: async function (joined) {
    var self = this;
    var url = joined.mediaServerUrl +
      "?auto_subscribe=0&sdk=js&version=1.6.7&protocol=8" +
      "&roomData=" + joined.roomData +
      "&clientVersion=1.0.0&clientPlatform=web";
    var sock = await ws.open(url, { "User-Agent": UA }, { readTimeoutMs: 45000 });
    this._sock = sock;

    sock.onclose = function (reason) {
      if (self._closed) return;
      setState("reconnecting", "socket closed: " + reason);
      self._clearTimers();
      self._teardownMedia();
      setTimeout(function () { self._connect(); }, 5000);
    };

    sock.onmessage = function (text) {
      var m;
      try { m = JSON.parse(text); } catch (e) { return; }
      if (m.joinResponse) {
        var jr = m.joinResponse;
        self._sid = jr.localParticipant ? jr.localParticipant.sid : "";
        self._setupPeer(jr.iceServers || []);
        setState("connected");
      } else if (m.offer && m.offer.sdp) {
        // webrtc disabled for test
      } else if (m.trickle && m.trickle.candidateInit) {
        if (m.trickle.target === "SUBSCRIBER" && self._pc) {
          var ci = JSON.parse(m.trickle.candidateInit);
          self._pc.addIceCandidate(ci);
        }
      } else if (m.newMessage) {
        self._onBus(m.newMessage);
      } else if (m.sidChanged && m.sidChanged.sid) {
        self._sid = m.sidChanged.sid;
      }
    };

  },

  _ensureTimers: function () {
    var self = this;
    if (!this._pingTimer) {
      this._pingTimer = setInterval(function () {
        self._rawSend({ pingReq: { timestamp: Date.now(), rtt: 0 } });
      }, 10000);
    }
    if (!this._flushTimer) {
      this._flushTimer = setInterval(function () { self._flush(); }, this._chunkMs);
    }
  },

  _clearTimers: function () {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._flushTimer) { clearInterval(this._flushTimer); this._flushTimer = null; }
  },

  _setupPeer: function (iceServers) {
    var self = this;
    this._teardownMedia();
    var pc = webrtc.newPeerConnection({ iceServers: iceServers });
    this._pc = pc;
    pc.onicecandidate = function (candidateInit) {
      if (candidateInit) {
        self._rawSend({ trickle: { candidateInit: candidateInit, target: "SUBSCRIBER" } });
      }
    };
    pc.onconnectionstatechange = function (state) {
      if (state === "failed") {
        setState("degraded", "peer connection failed");
      }
    };
    pc.ondatachannel = function () {};
    this._ensureTimers();
  },

  _answer: async function (sdp) {
    if (!this._pc) return;
    try {
      await this._pc.setRemoteDescription("offer", sdp);
      var answer = await this._pc.createAnswer();
      await this._pc.setLocalDescription("answer", answer);
      this._rawSend({ answer: { type: "answer", sdp: answer } });
    } catch (e) {
      setState("degraded", "answer: " + String(e));
    }
  },

  _teardownMedia: function () {
    if (this._pc) {
      this._pc.close();
      this._pc = null;
    }
  },

  _rawSend: function (obj) {
    if (this._sock) {
      this._sock.send(JSON.stringify(obj));
    }
  },

  _onBus: function (nm) {
    var msg = nm.message;
    if (typeof msg !== "string" || msg.indexOf('"' + EVENT + '"') < 0) return;
    var parsed;
    try { parsed = JSON.parse(msg); } catch (e) { return; }
    if (!parsed || parsed.eventName !== EVENT || !parsed.d) return;
    if (parsed.sid === this._sid) return;
    var bytes = base64.decode(parsed.d);
    emit(parsed.z ? gzip.decompress(bytes) : bytes);
  },

  write: function (bytes) {
    this._buf.push(new Uint8Array(bytes));
    this._bufLen += bytes.byteLength;
  },

  _flush: function () {
    if (!this._sock || this._bufLen === 0) return;
    try {
      var chunk = takeBytes(this._buf, Math.min(CHUNK_BYTES, this._bufLen));
      this._bufLen -= chunk.length;
      this._rawSend({
        sendMessage: {
          message: JSON.stringify({
            eventName: EVENT,
            sid: this._sid,
            z: 0,
            d: base64.encode(chunk),
          }),
        },
      });
    } catch (e) {
      setState("degraded", "flush: " + String(e));
    }
  },

  close: function () {
    this._closed = true;
    this._clearTimers();
    this._teardownMedia();
    if (this._sock) {
      this._sock.close();
      this._sock = null;
    }
  },

  onEvent: function (kind) {
    if (kind === "cookiesApplied") {
      setState("reconnecting", "cookies applied");
      this._teardownMedia();
      if (this._sock) { this._sock.close(); this._sock = null; }
      var self = this;
      setTimeout(function () { self._connect(); }, 1000);
    }
  },
};

function pad64(s) {
  while (s.length % 4 !== 0) s += "=";
  return s;
}

function takeBytes(queue, n) {
  var out = new Uint8Array(n);
  var filled = 0;
  while (filled < n && queue.length > 0) {
    var head = queue[0];
    var want = n - filled;
    if (head.length <= want) {
      out.set(head, filled);
      filled += head.length;
      queue.shift();
    } else {
      out.set(head.subarray(0, want), filled);
      queue[0] = head.subarray(want);
      filled = n;
    }
  }
  return out;
}
