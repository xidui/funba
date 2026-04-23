import Constants from "expo-constants";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { useAppStore } from "./store";

const configuredBase = (Constants.expoConfig?.extra as { apiBaseUrl?: string } | undefined)?.apiBaseUrl;
export const API_BASE_URL = configuredBase?.replace(/\/$/, "") ?? "http://localhost:5001";

const TOKEN_KEY = "funba.bearer";

export async function setAuthToken(token: string | null): Promise<void> {
  if (token) {
    await AsyncStorage.setItem(TOKEN_KEY, token);
  } else {
    await AsyncStorage.removeItem(TOKEN_KEY);
  }
}

export async function getAuthToken(): Promise<string | null> {
  return AsyncStorage.getItem(TOKEN_KEY);
}

type Query = Record<string, string | number | boolean | null | undefined>;

function buildUrl(path: string, query?: Query): string {
  const url = new URL(`${API_BASE_URL}${path}`);
  const lang = useAppStore.getState().lang;
  url.searchParams.set("lang", lang);
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v === null || v === undefined) continue;
      url.searchParams.set(k, String(v));
    }
  }
  return url.toString();
}

export class ApiError extends Error {
  constructor(public status: number, public body: unknown, message?: string) {
    super(message ?? `HTTP ${status}`);
  }
}

export async function apiGet<T = unknown>(path: string, query?: Query): Promise<T> {
  const token = await getAuthToken();
  const res = await fetch(buildUrl(path, query), {
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
  });
  const body = await res.json().catch(() => null);
  if (!res.ok) {
    throw new ApiError(res.status, body);
  }
  return body as T;
}

export async function apiPost<T = unknown>(path: string, body: unknown, query?: Query): Promise<T> {
  const token = await getAuthToken();
  const res = await fetch(buildUrl(path, query), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body ?? {}),
  });
  const parsed = await res.json().catch(() => null);
  if (!res.ok) {
    throw new ApiError(res.status, parsed);
  }
  return parsed as T;
}
