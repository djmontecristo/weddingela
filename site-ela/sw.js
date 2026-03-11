self.addEventListener('install', (event) => {
  console.log('Service Worker installed');
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  console.log('Service Worker activated');
});

self.addEventListener('push', (event) => {
  let data = {};

  try {
    data = event.data.json();
  } catch (e) {
    data = { title: "WeddingEla", body: "Νέα ειδοποίηση" };
  }

  const title = data.title || "WeddingEla";
  const options = {
    body: data.body || "Νέο ενδιαφέρον",
    icon: "/icon-192.png",
    badge: "/icon-192.png"
  };

event.waitUntil((async () => {
  await self.registration.showNotification(title, options);

if ('setAppBadge' in self.navigator && data.badge) {
  try {
    await self.navigator.setAppBadge(data.badge);
  } catch (e) {}
}
})());
});