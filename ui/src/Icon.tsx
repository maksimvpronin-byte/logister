const paths = {
  logo: "M5 6h14M5 12h10M5 18h6m5-2 3 2-3 2",
  search: "m21 21-4.3-4.3M19 11a8 8 0 1 1-16 0 8 8 0 0 1 16 0Z",
  streams: "M4 5h16v4H4zM4 15h16v4H4zM8 7h.01M8 17h.01",
  access: "M12 22s7-3.5 7-9V6l-7-3-7 3v7c0 5.5 7 9 7 9Zm-3-10 2 2 4-4",
  activity: "M3 12h4l3-7 4 14 3-7h4",
  account: "M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2m12-14a4 4 0 1 1-8 0 4 4 0 0 1 8 0Z",
  logout: "M10 17l5-5-5-5m5 5H3m12-8h5v16h-5",
  arrow: "m9 18 6-6-6-6",
  copy: "M8 8h12v12H8zM16 4H4v12",
  close: "m6 6 12 12M18 6 6 18",
  refresh: "M20 7v5h-5m4.5-1A7.5 7.5 0 1 0 18 18",
  inbox: "M4 4h16l2 10v6H2v-6L4 4Zm-2 10h6l2 2h4l2-2h6",
  input: "M4 4h16v12H4zM8 20h8m-4-7V7m-2.5 3.5L12 13l2.5-2.5",
  write: "M4 4h16v12H4zM8 20h8m-7-9 2 2 4-4",
  clock: "M12 7v5l3 2m6-2a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z",
};

export type IconName = keyof typeof paths;

export default function Icon({ name }: { name: IconName }) {
  return <svg className="ui-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false"><path d={paths[name]} /></svg>;
}
