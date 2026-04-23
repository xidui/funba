import "../global.css";

import { useEffect } from "react";
import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { SafeAreaProvider } from "react-native-safe-area-context";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import * as Linking from "expo-linking";
import { colors } from "../lib/theme";
import { useAppStore } from "../lib/store";
import { refreshMe, verifyMagicToken } from "../lib/auth";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 60_000,
      retry: 1,
    },
  },
});

export default function RootLayout() {
  const hydrate = useAppStore((s) => s.hydrate);

  useEffect(() => {
    hydrate().then(() => refreshMe());
  }, [hydrate]);

  // Handle funba://auth?token=... magic-link callback
  useEffect(() => {
    const handle = async (url: string) => {
      try {
        const parsed = Linking.parse(url);
        const token = (parsed.queryParams as any)?.token;
        if (parsed.hostname === "auth" && typeof token === "string" && token) {
          await verifyMagicToken(token);
        }
      } catch {}
    };
    Linking.getInitialURL().then((url) => {
      if (url) handle(url);
    });
    const sub = Linking.addEventListener("url", (e) => handle(e.url));
    return () => sub.remove();
  }, []);

  return (
    <GestureHandlerRootView style={{ flex: 1, backgroundColor: colors.bg }}>
      <SafeAreaProvider>
        <QueryClientProvider client={queryClient}>
          <StatusBar style="light" />
          <Stack
            screenOptions={{
              headerStyle: { backgroundColor: colors.bg },
              headerTintColor: colors.text,
              headerTitleStyle: { color: colors.text, fontFamily: "Outfit" },
              contentStyle: { backgroundColor: colors.bg },
            }}
          >
            <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
            <Stack.Screen name="games/[slug]" options={{ title: "Game" }} />
            <Stack.Screen name="players/[slug]" options={{ title: "Player" }} />
            <Stack.Screen name="teams/[slug]" options={{ title: "Team" }} />
            <Stack.Screen name="compare" options={{ title: "Compare" }} />
            <Stack.Screen name="search" options={{ title: "Search" }} />
            <Stack.Screen name="metrics/[key]" options={{ title: "Metric" }} />
            <Stack.Screen name="metrics/mine" options={{ title: "My Metrics" }} />
            <Stack.Screen name="news" options={{ title: "News" }} />
            <Stack.Screen name="news/[id]" options={{ title: "Story" }} />
            <Stack.Screen name="draft/[year]" options={{ title: "Draft" }} />
            <Stack.Screen name="awards" options={{ title: "Awards" }} />
            <Stack.Screen name="teams" options={{ title: "Teams" }} />
            <Stack.Screen name="players" options={{ title: "Players" }} />
            <Stack.Screen name="login" options={{ title: "Sign in", presentation: "modal" }} />
          </Stack>
        </QueryClientProvider>
      </SafeAreaProvider>
    </GestureHandlerRootView>
  );
}
