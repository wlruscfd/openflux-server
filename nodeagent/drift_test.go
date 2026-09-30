package nodeagent

import "testing"

func TestWorkerDriftDetectsEveryPanelEditThatChangesTheTransport(t *testing.T) {
	base := RemoteKey{
		ID:            "k1",
		Transport:     "mts",
		DocURL:        "https://example/board/1",
		E2EEncryption: false,
	}
	w := &worker{
		docURL:        base.DocURL,
		transportName: base.Transport,
		docURLs:       append([]string(nil), base.DocURLs...),
		e2e:           base.E2EEncryption,
	}
	if w.driftedFrom(base) {
		t.Fatal("a worker built from the same values must not count as drifted")
	}

	edited := base
	edited.DocURL = "https://example/board/2"
	if !w.driftedFrom(edited) {
		t.Error("a changed doc_url must restart the worker")
	}

	edited = base
	edited.Transport = "mailru"
	if !w.driftedFrom(edited) {
		t.Error("a changed transport must restart the worker")
	}

	edited = base
	edited.E2EEncryption = true
	if !w.driftedFrom(edited) {
		t.Error("toggling e2e_encryption must restart the worker")
	}

	edited = base
	edited.DocURLs = []string{"https://example/a", "https://example/b"}
	if !w.driftedFrom(edited) {
		t.Error("added doc_urls must restart the worker")
	}
}

func TestWorkerDriftDetectsReorderedDocURLs(t *testing.T) {
	urls := []string{"https://example/a", "https://example/b"}
	w := &worker{
		docURL:        "https://example/main",
		transportName: "yandex_multistream",
		docURLs:       append([]string(nil), urls...),
	}
	same := RemoteKey{ID: "k", Transport: "yandex_multistream", DocURL: "https://example/main", DocURLs: append([]string(nil), urls...)}
	if w.driftedFrom(same) {
		t.Error("identical doc_urls in the same order must not count as drift")
	}

	reordered := RemoteKey{ID: "k", Transport: "yandex_multistream", DocURL: "https://example/main", DocURLs: []string{urls[1], urls[0]}}
	if !w.driftedFrom(reordered) {
		t.Error("a reordered doc_urls list must restart the worker - stream index i must keep naming the same document on both ends")
	}

	shortened := RemoteKey{ID: "k", Transport: "yandex_multistream", DocURL: "https://example/main", DocURLs: urls[:1]}
	if !w.driftedFrom(shortened) {
		t.Error("removing a doc_url must restart the worker")
	}
}
