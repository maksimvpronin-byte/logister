import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import ThemePicker, { restoreTheme } from "./ThemePicker";

afterEach(() => {
  localStorage.removeItem("logister.theme");
  delete document.documentElement.dataset.theme;
});

it("restores the selected theme for a newly mounted picker", () => {
  restoreTheme();
  const view = render(<ThemePicker />);
  const picker = screen.getByLabelText("Тема оформления");
  fireEvent.change(picker, { target: { value: "gzhel" } });
  expect(document.documentElement).toHaveAttribute("data-theme", "gzhel");
  view.unmount();
  delete document.documentElement.dataset.theme;

  restoreTheme();
  render(<ThemePicker />);
  expect(screen.getByLabelText("Тема оформления")).toHaveValue("gzhel");
});

it("falls back to Graphite if the stored theme is unknown", () => {
  localStorage.setItem("logister.theme", "obsolete");
  restoreTheme();
  expect(document.documentElement).toHaveAttribute("data-theme", "classic");
});
