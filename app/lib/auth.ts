import { apiGet, apiPost, setAuthToken } from "./api";
import { useAppStore } from "./store";

type MeResponse = { user: ReturnType<typeof useAppStore.getState>["user"] };

export async function refreshMe(): Promise<void> {
  try {
    const res = await apiGet<MeResponse>("/api/v1/mobile/me");
    useAppStore.getState().setUser(res.user ?? null);
  } catch {
    useAppStore.getState().setUser(null);
  }
}

export async function requestMagicLink(email: string): Promise<void> {
  await apiPost("/api/v1/mobile/auth/magic/request", { email });
}

type VerifyResponse = {
  token: string;
  user: NonNullable<ReturnType<typeof useAppStore.getState>["user"]>;
};

export async function verifyMagicToken(token: string): Promise<void> {
  const res = await apiPost<VerifyResponse>("/api/v1/mobile/auth/magic/verify", { token });
  await setAuthToken(res.token);
  useAppStore.getState().setUser(res.user);
}

export async function signOut(): Promise<void> {
  await setAuthToken(null);
  useAppStore.getState().setUser(null);
}
