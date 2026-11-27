/**
 * Bootstrap. A content script cannot be an ES module and there is no build step
 * here, so this does one thing: check the page really is a Mirobody instance,
 * then import the two modules from the extension origin. `chat-share.js` mounts
 * itself and keeps itself mounted; nothing else is needed.
 *
 * The check is `/mirobody.json` — every instance serves it, and an unrelated
 * site answers 404 or an unrelated page. It costs one request on the hosts the
 * manifest lists.
 */

(async () => {
  const flag = await fetch("/mirobody.json", { headers: { Accept: "application/json" } })
    .then((response) =>
      response.ok && (response.headers.get("content-type") || "").includes("json")
        ? response.json()
        : null,
    )
    .catch(() => null);

  if (!flag || !("server_version" in flag)) return;

  try {
    await import(chrome.runtime.getURL("chat-share.js"));
  } catch (error) {
    console.warn("[mirobody] conversation export failed to load", error);
  }
})();
