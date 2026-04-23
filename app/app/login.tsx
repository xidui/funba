import { useState } from "react";
import { router } from "expo-router";
import { Linking, Pressable, Text, TextInput, View, ActivityIndicator } from "react-native";
import { Screen } from "../components/Screen";
import { Card } from "../components/Card";
import { requestMagicLink } from "../lib/auth";
import { API_BASE_URL } from "../lib/api";
import { colors } from "../lib/theme";
import { t } from "../lib/i18n";

export default function LoginScreen() {
  const [email, setEmail] = useState("");
  const [state, setState] = useState<"idle" | "sending" | "sent" | "error">("idle");

  const onSend = async () => {
    if (!email.includes("@")) return;
    setState("sending");
    try {
      await requestMagicLink(email.trim().toLowerCase());
      setState("sent");
    } catch {
      setState("error");
    }
  };

  return (
    <Screen>
      <Text className="text-text font-head text-3xl mb-1">Funba</Text>
      <Text className="text-muted font-sans text-xs mb-6">{t("login")}</Text>

      {state === "sent" ? (
        <Card>
          <Text className="text-text font-head text-lg">{t("link_sent")}</Text>
          <Text className="text-muted font-sans text-xs mt-2">{email}</Text>
          <Pressable onPress={() => router.back()} className="bg-surface2 border border-border rounded-full px-4 py-3 mt-4 items-center">
            <Text className="text-text font-head">OK</Text>
          </Pressable>
        </Card>
      ) : (
        <Card>
          <Text className="text-text font-head mb-2">{t("email")}</Text>
          <TextInput
            value={email}
            onChangeText={setEmail}
            placeholder="you@example.com"
            placeholderTextColor={colors.muted}
            autoCapitalize="none"
            autoComplete="email"
            keyboardType="email-address"
            className="bg-surface2 border border-border rounded-xl px-3 py-3 text-text font-sans"
          />
          <Pressable
            onPress={onSend}
            disabled={state === "sending" || !email.includes("@")}
            className="bg-accent rounded-full px-4 py-3 mt-4 items-center"
          >
            {state === "sending" ? (
              <ActivityIndicator color="#000" />
            ) : (
              <Text className="text-black font-head">{t("send_link")}</Text>
            )}
          </Pressable>
          {state === "error" && (
            <Text className="text-loss font-sans text-xs mt-3 text-center">{t("error_generic")}</Text>
          )}

          <View className="border-t border-border mt-6 pt-4 items-center">
            <Text className="text-muted font-sans text-xs mb-2">Or sign in with Google on the web</Text>
            <Pressable
              onPress={() => Linking.openURL(`${API_BASE_URL}/auth/login`)}
              className="bg-surface2 border border-border rounded-full px-4 py-2"
            >
              <Text className="text-text font-head text-xs">Open web sign-in ↗</Text>
            </Pressable>
          </View>
        </Card>
      )}
    </Screen>
  );
}
