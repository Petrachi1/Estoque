self.addEventListener("install", (e) => {
  self.skipWaiting();
});
self.addEventListener("activate", (e) => {
  clients.claim();
});
self.addEventListener("fetch", (e) => {
  // network-first simples
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
