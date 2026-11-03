import { useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { FieldSearch } from "./fieldSearch";

export default function FieldActionsMenu({ anchor, path, search, onSelect, onClose }: {
  anchor: HTMLButtonElement; path: string; search: FieldSearch;
  onSelect: (exclude: boolean) => void; onClose: () => void;
}) {
  const menu = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: 0, top: 0, visible: false });
  useLayoutEffect(() => {
    const panel = menu.current!;
    const place = () => {
      const rect = anchor.getBoundingClientRect();
      const clip = anchor.closest(".fields-scroll")?.getBoundingClientRect();
      if (!anchor.isConnected || rect.bottom <= 0 || rect.top >= window.innerHeight
        || (clip && clip.height > 0 && (rect.bottom <= clip.top || rect.top >= clip.bottom))) { onClose(); return; }
      setPosition({
        left: Math.max(8, Math.min(rect.right - panel.offsetWidth, window.innerWidth - panel.offsetWidth - 8)),
        top: Math.max(8, rect.bottom + panel.offsetHeight + 6 < window.innerHeight ? rect.bottom + 6 : rect.top - panel.offsetHeight - 6),
        visible: true,
      });
    };
    place();
    (panel.querySelector<HTMLButtonElement>('button:not(:disabled)') ?? panel).focus({ preventScroll: true });
    const outside = (event: PointerEvent) => {
      if (!panel.contains(event.target as Node) && !anchor.contains(event.target as Node)) onClose();
    };
    const scroll = (event: Event) => { if (!panel.contains(event.target as Node)) place(); };
    document.addEventListener("pointerdown", outside, true);
    window.addEventListener("scroll", scroll, true);
    window.addEventListener("resize", place);
    return () => {
      document.removeEventListener("pointerdown", outside, true);
      window.removeEventListener("scroll", scroll, true);
      window.removeEventListener("resize", place);
    };
  }, [anchor, onClose]);
  return createPortal(<div ref={menu} className="field-actions-menu" role="menu" aria-label={`Действия с полем ${path}`} tabIndex={-1}
    style={{ left: position.left, top: position.top, visibility: position.visible ? "visible" : "hidden" }}
    onKeyDown={event => {
      if (event.key === "Escape" || event.key === "Tab") {
        if (event.key === "Escape") { event.preventDefault(); event.stopPropagation(); }
        onClose(); anchor.focus(); return;
      }
      if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      const items = [...menu.current!.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')];
      const current = items.indexOf(document.activeElement as HTMLButtonElement);
      const next = event.key === "Home" ? 0 : event.key === "End" ? items.length - 1 : (current + (event.key === "ArrowDown" ? 1 : -1) + items.length) % items.length;
      items[next]?.focus();
    }}>
    <div className="field-menu-heading">{path}</div>
    <button type="button" role="menuitem" disabled={!search.clause} onClick={() => { onSelect(false); anchor.focus(); }}><span aria-hidden="true">＋</span> Добавить в поиск</button>
    <button type="button" role="menuitem" disabled={!search.clause} onClick={() => { onSelect(true); anchor.focus(); }}><span aria-hidden="true">−</span> Исключить из поиска</button>
    {search.reason && <p className="field-menu-reason">{search.reason}</p>}
  </div>, document.body);
}
