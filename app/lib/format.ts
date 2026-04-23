export function pct(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined) return "-";
  return `${(value * 100).toFixed(digits)}%`;
}

export function num(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined) return "-";
  return Number(value).toFixed(digits);
}

export function int(value: number | null | undefined): string {
  if (value === null || value === undefined) return "-";
  return String(Math.round(value));
}

export function resultColor(result: "W" | "L" | "-" | string): string {
  if (result === "W") return "text-win";
  if (result === "L") return "text-loss";
  return "text-muted";
}

export function plusMinusColor(value: number | null | undefined): string {
  if (value === null || value === undefined || value === 0) return "text-muted";
  return value > 0 ? "text-win" : "text-loss";
}

export function teamScoreClass(winner: boolean): string {
  return winner ? "text-text font-bold" : "text-muted";
}

export function formatDate(iso: string | null | undefined, lang: "en" | "zh" = "en"): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  if (lang === "zh") {
    return `${d.getFullYear()}年${d.getMonth() + 1}月${d.getDate()}日`;
  }
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

export function formatRelative(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  const diffSec = (Date.now() - d.getTime()) / 1000;
  if (diffSec < 60) return "just now";
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}
