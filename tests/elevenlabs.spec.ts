import { expect, test } from "@playwright/test";

const harness = `<!doctype html><html><head><script type="module">
import RefreshRuntime from '/@react-refresh';
RefreshRuntime.injectIntoGlobalHook(window);
window.$RefreshReg$ = () => {};
window.$RefreshSig$ = () => (type) => type;
window.__vite_plugin_react_preamble_installed__ = true;
</script></head><body><div id="test-root"></div><script type="module">
import React from '/node_modules/.vite/deps/react.js';
import ReactDOM from '/node_modules/.vite/deps/react-dom_client.js';
import i18n from '/src/i18n/index.ts';
import '/src/App.css';
import { useSettingsStore } from '/src/stores/settingsStore.ts';
import { ElevenLabsSettings } from '/src/components/settings/models/ElevenLabsSettings.tsx';
await i18n.changeLanguage('en');
const settings = () => ({selected_model:'local-model',elevenlabs_enabled:window.cloud.enabled,elevenlabs_model:window.cloud.model});
useSettingsStore.setState({isLoading:false,settings:settings(),refreshSettings:async()=>{useSettingsStore.setState({settings:settings()})}});
ReactDOM.createRoot(document.getElementById('test-root')).render(React.createElement(ElevenLabsSettings,{onActivated:()=>{window.activated=true}}));
</script></body></html>`;

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    const w = window as unknown as Record<string, unknown>;
    const cloud = { enabled: false, model: "scribe_v2", has_api_key: false };
    w.cloud = cloud;
    w.calls = [];
    w.activated = false;
    w.__TAURI_INTERNALS__ = {
      invoke: async (command: string, args: Record<string, unknown>) => {
        if (command === "get_elevenlabs_status") return { ...cloud };
        if (command === "configure_elevenlabs") {
          if (w.failSave) throw "Credential store locked";
          (w.calls as unknown[]).push(args);
          cloud.enabled = args.enabled as boolean;
          cloud.model = args.model as string;
          if (args.apiKey) cloud.has_api_key = true;
          return { ...cloud };
        }
        if (command === "remove_elevenlabs_key") {
          cloud.enabled = false;
          cloud.has_api_key = false;
          return null;
        }
        if (command === "plugin:os|locale") return "en-US";
        if (command === "get_app_settings") return { app_language: "en" };
        return null;
      },
    };
  });
  await page.route("**/__cloud_test__", (route) =>
    route.fulfill({ contentType: "text/html", body: harness }),
  );
  await page.goto("/__cloud_test__");
});

test("activates chosen cloud model, clears secret field, and can return to local", async ({
  page,
}) => {
  const save = page.getByRole("button", { name: "Save and use ElevenLabs" });
  await expect(save).toBeDisabled();
  await page
    .getByLabel("Transcription model", { exact: true })
    .selectOption("scribe_v1");
  await page
    .getByLabel("ElevenLabs API key", { exact: true })
    .fill("fake-test-key");
  await save.click();
  await expect(
    page.getByText("ElevenLabs · scribe_v1", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByLabel("ElevenLabs API key", { exact: true }),
  ).toHaveValue("");
  expect(await page.evaluate("window.calls[0]")).toEqual({
    enabled: true,
    model: "scribe_v1",
    apiKey: "fake-test-key",
  });
  expect(await page.evaluate("window.activated")).toBe(true);
  await page.getByRole("button", { name: "Use previous local model" }).click();
  await expect(
    page.getByText("ElevenLabs · scribe_v1", { exact: true }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Remove saved key" }).click();
  await expect(save).toBeDisabled();
});

test("credential errors keep onboarding incomplete and show a useful error", async ({
  page,
}) => {
  await page.evaluate("window.failSave = true");
  await page
    .getByLabel("ElevenLabs API key", { exact: true })
    .fill("fake-test-key");
  await page.getByRole("button", { name: "Save and use ElevenLabs" }).click();
  await expect(page.getByRole("alert")).toHaveText("Credential store locked");
  expect(await page.evaluate("window.activated")).toBe(false);
});
