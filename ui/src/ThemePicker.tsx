import { useState } from "react";

const themes = [
  { id: "classic", label: "Графит" },
  { id: "khokhloma", label: "Хохлома" },
  { id: "gzhel", label: "Гжель" },
  { id: "gorodets", label: "Городецкая роспись" },
] as const;

type Theme = (typeof themes)[number]["id"];
const storageKey = "logister.theme";

function themeId(value: string | null | undefined): Theme {
  return themes.find(theme => theme.id === value)?.id ?? "classic";
}

export function restoreTheme() {
  let stored: string | null = null;
  try { stored = window.localStorage.getItem(storageKey); }
  catch { /* The default theme works when browser storage is blocked. */ }
  document.documentElement.dataset.theme = themeId(stored);
}

export default function ThemePicker() {
  const [theme, setTheme] = useState(() => themeId(document.documentElement.dataset.theme));

  function change(value: string) {
    const next = themeId(value);
    document.documentElement.dataset.theme = next;
    setTheme(next);
    try { window.localStorage.setItem(storageKey, next); }
    catch { /* The active theme still applies for this page. */ }
  }

  return <label className="theme-picker">
    <span className="theme-swatch" aria-hidden="true" />
    <span>Тема</span>
    <select value={theme} aria-label="Тема оформления" onChange={event => change(event.target.value)}>
      {themes.map(({ id, label }) => <option key={id} value={id}>{label}</option>)}
    </select>
  </label>;
}
