import { create } from "zustand";
import AsyncStorage from "@react-native-async-storage/async-storage";

export type Lang = "en" | "zh";

type User = {
  id: string;
  email: string;
  display_name: string;
  avatar_url: string | null;
  subscription_tier: "free" | "pro";
  subscription_status?: string | null;
};

type AppState = {
  lang: Lang;
  user: User | null;
  hydrated: boolean;
  setLang: (lang: Lang) => void;
  setUser: (user: User | null) => void;
  hydrate: () => Promise<void>;
};

const LANG_KEY = "funba.lang";

export const useAppStore = create<AppState>((set) => ({
  lang: "en",
  user: null,
  hydrated: false,
  setLang: (lang) => {
    set({ lang });
    AsyncStorage.setItem(LANG_KEY, lang).catch(() => {});
  },
  setUser: (user) => set({ user }),
  hydrate: async () => {
    try {
      const stored = await AsyncStorage.getItem(LANG_KEY);
      if (stored === "en" || stored === "zh") {
        set({ lang: stored });
      }
    } finally {
      set({ hydrated: true });
    }
  },
}));
