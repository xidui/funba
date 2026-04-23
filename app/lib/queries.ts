import { useQuery } from "@tanstack/react-query";
import { apiGet } from "./api";
import { useAppStore } from "./store";

// Query factory — lang is part of the key so switching languages refetches.
function key(path: string, params?: Record<string, unknown>) {
  const lang = useAppStore.getState().lang;
  return [path, lang, params ?? {}] as const;
}

export function useHome(season?: string) {
  return useQuery({
    queryKey: key("/api/v1/mobile/home", { season }),
    queryFn: () => apiGet<any>("/api/v1/mobile/home", { season }),
  });
}

export function useGamesList(params: { year?: string; phase?: string; team?: string; page?: number }) {
  return useQuery({
    queryKey: key("/api/v1/mobile/games", params),
    queryFn: () => apiGet<any>("/api/v1/mobile/games", params),
  });
}

export function useGameDetail(slugOrId: string | undefined) {
  return useQuery({
    enabled: Boolean(slugOrId),
    queryKey: key(`/api/v1/mobile/games/${slugOrId}`),
    queryFn: () => apiGet<any>(`/api/v1/mobile/games/${slugOrId}`),
  });
}

export function useTeamsList() {
  return useQuery({
    queryKey: key("/api/v1/mobile/teams"),
    queryFn: () => apiGet<any>("/api/v1/mobile/teams"),
  });
}

export function useTeamDetail(slugOrId: string | undefined, season?: string) {
  return useQuery({
    enabled: Boolean(slugOrId),
    queryKey: key(`/api/v1/mobile/teams/${slugOrId}`, { season }),
    queryFn: () => apiGet<any>(`/api/v1/mobile/teams/${slugOrId}`, { season }),
  });
}

export function usePlayersBrowse(params: { season?: string; team?: string }) {
  return useQuery({
    queryKey: key("/api/v1/mobile/players", params),
    queryFn: () => apiGet<any>("/api/v1/mobile/players", params),
  });
}

export function usePlayerDetail(slugOrId: string | undefined, season?: string) {
  return useQuery({
    enabled: Boolean(slugOrId),
    queryKey: key(`/api/v1/mobile/players/${slugOrId}`, { season }),
    queryFn: () => apiGet<any>(`/api/v1/mobile/players/${slugOrId}`, { season }),
  });
}

export function usePlayerCompare(ids: string[]) {
  const idsStr = ids.join(",");
  return useQuery({
    enabled: ids.length >= 2,
    queryKey: key("/api/v1/mobile/players/compare", { ids: idsStr }),
    queryFn: () => apiGet<any>("/api/v1/mobile/players/compare", { ids: idsStr }),
  });
}

export function usePlayerHints(q: string) {
  return useQuery({
    enabled: q.length >= 2,
    queryKey: key("/api/v1/mobile/players/hints", { q }),
    queryFn: () => apiGet<any>("/api/v1/mobile/players/hints", { q }),
  });
}

export function useMetricsBrowse(params: { scope?: string; q?: string }) {
  return useQuery({
    queryKey: key("/api/v1/mobile/metrics", params),
    queryFn: () => apiGet<any>("/api/v1/mobile/metrics", params),
  });
}

export function useMetricDetail(metricKey: string | undefined, params: { season?: string; page?: number }) {
  return useQuery({
    enabled: Boolean(metricKey),
    queryKey: key(`/api/v1/mobile/metrics/${metricKey}`, params),
    queryFn: () => apiGet<any>(`/api/v1/mobile/metrics/${metricKey}`, params),
  });
}

export function useMyMetrics() {
  return useQuery({
    queryKey: key("/api/v1/mobile/metrics/mine"),
    queryFn: () => apiGet<any>("/api/v1/mobile/metrics/mine"),
  });
}

export function useNewsList() {
  return useQuery({
    queryKey: key("/api/v1/mobile/news"),
    queryFn: () => apiGet<any>("/api/v1/mobile/news"),
  });
}

export function useNewsDetail(clusterId: string | number | undefined) {
  return useQuery({
    enabled: Boolean(clusterId),
    queryKey: key(`/api/v1/mobile/news/${clusterId}`),
    queryFn: () => apiGet<any>(`/api/v1/mobile/news/${clusterId}`),
  });
}

export function useDraft(year: number | string | undefined) {
  return useQuery({
    enabled: Boolean(year),
    queryKey: key(`/api/v1/mobile/draft/${year}`),
    queryFn: () => apiGet<any>(`/api/v1/mobile/draft/${year}`),
  });
}

export function useAwards(type: string) {
  return useQuery({
    queryKey: key("/api/v1/mobile/awards", { type }),
    queryFn: () => apiGet<any>("/api/v1/mobile/awards", { type }),
  });
}
