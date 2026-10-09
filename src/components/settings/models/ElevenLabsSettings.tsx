import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { useSettings } from "@/hooks/useSettings";

type CloudStatus = { enabled: boolean; model: string; has_api_key: boolean };

export function ElevenLabsSettings({
  onActivated,
  disabled = false,
}: {
  onActivated?: () => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const { settings, refreshSettings } = useSettings();
  const [status, setStatus] = useState<CloudStatus | null>(null);
  const [model, setModel] = useState("scribe_v2");
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    invoke<CloudStatus>("get_elevenlabs_status")
      .then((value) => {
        if (cancelled) return;
        setStatus(value);
        setModel(value.model);
        setError("");
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [settings?.elevenlabs_enabled, settings?.elevenlabs_model]);

  const save = async (enabled: boolean) => {
    setBusy(true);
    setError("");
    try {
      const value = await invoke<CloudStatus>("configure_elevenlabs", {
        enabled,
        model,
        apiKey: key.trim() || null,
      });
      setKey("");
      setStatus(value);
      await refreshSettings();
      toast.success(
        t(enabled ? "elevenlabs.activated" : "elevenlabs.localSelected"),
      );
      if (enabled) onActivated?.();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const forget = async () => {
    setBusy(true);
    try {
      await invoke("remove_elevenlabs_key");
      setKey("");
      setStatus({ enabled: false, model, has_api_key: false });
      await refreshSettings();
      setError("");
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const locked = busy || disabled || !status;
  return (
    <section
      className="rounded-xl border border-logo-primary/30 bg-logo-primary/5 p-5 space-y-4 text-start"
      aria-label={t("elevenlabs.title")}
    >
      <div>
        <h2 className="font-semibold">{t("elevenlabs.title")}</h2>
        <p className="text-sm text-text/70 mt-1">
          {t("elevenlabs.description")}
        </p>
      </div>
      <label className="block text-sm space-y-1">
        <span>{t("elevenlabs.model")}</span>
        <select
          aria-label={t("elevenlabs.model")}
          value={model}
          disabled={locked}
          onChange={(e) => setModel(e.target.value)}
          className="block w-full rounded-md border border-mid-gray/40 bg-background p-2"
        >
          <option value="scribe_v2">{t("elevenlabs.scribeV2")}</option>
          <option value="scribe_v1">{t("elevenlabs.scribeV1")}</option>
        </select>
      </label>
      <label className="block text-sm space-y-1">
        <span>{t("elevenlabs.apiKey")}</span>
        <input
          type="password"
          autoComplete="off"
          spellCheck={false}
          aria-label={t("elevenlabs.apiKey")}
          value={key}
          disabled={locked}
          onChange={(e) => setKey(e.target.value)}
          placeholder={t(
            status?.has_api_key
              ? "elevenlabs.replaceKey"
              : "elevenlabs.enterKey",
          )}
          className="block w-full rounded-md border border-mid-gray/40 bg-background p-2"
        />
      </label>
      <p className="text-xs text-text/60">{t("elevenlabs.keyStorage")}</p>
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          disabled={locked || (!key.trim() && !status?.has_api_key)}
          onClick={() => save(true)}
          className="rounded-lg bg-logo-primary text-white px-4 py-2 text-sm disabled:opacity-40"
        >
          {t(busy ? "elevenlabs.saving" : "elevenlabs.activate")}
        </button>
        {status?.enabled && settings?.selected_model && (
          <button
            type="button"
            disabled={locked}
            onClick={() => save(false)}
            className="rounded-lg border border-mid-gray/40 px-4 py-2 text-sm disabled:opacity-40"
          >
            {t("elevenlabs.useLocal")}
          </button>
        )}
        {status?.has_api_key && (
          <button
            type="button"
            disabled={locked}
            onClick={forget}
            className="rounded-lg border border-mid-gray/40 px-4 py-2 text-sm disabled:opacity-40"
          >
            {t("elevenlabs.forgetKey")}
          </button>
        )}
      </div>
      {status?.enabled && (
        <p className="text-sm font-medium">
          {t("elevenlabs.activeModel", { model: status.model })}
        </p>
      )}
      {error && (
        <p role="alert" className="text-sm text-red-500">
          {error}
        </p>
      )}
    </section>
  );
}
