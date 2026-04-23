import { useState } from "react";
import { Link, router } from "expo-router";
import { Linking, Pressable, Text, TextInput, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, PillButton, SectionTitle } from "../../components/Card";
import { useAppStore } from "../../lib/store";
import { signOut } from "../../lib/auth";
import { apiPost } from "../../lib/api";
import { API_BASE_URL } from "../../lib/api";
import { colors } from "../../lib/theme";
import { t } from "../../lib/i18n";

export default function AccountTab() {
  const user = useAppStore((s) => s.user);
  const lang = useAppStore((s) => s.lang);
  const setLang = useAppStore((s) => s.setLang);
  const [feedback, setFeedback] = useState("");
  const [feedbackState, setFeedbackState] = useState<"idle" | "sent" | "error">("idle");

  const sendFeedback = async () => {
    if (!feedback.trim()) return;
    try {
      await apiPost("/api/v1/mobile/feedback", { content: feedback.trim() });
      setFeedback("");
      setFeedbackState("sent");
    } catch {
      setFeedbackState("error");
    }
  };

  return (
    <Screen>
      <Text className="text-text font-head text-2xl mb-3">{t("account")}</Text>

      {user ? (
        <Card>
          <Text className="text-text font-head text-lg">{user.display_name}</Text>
          <Text className="text-muted font-sans text-xs mt-1">{user.email}</Text>
          <View className="flex-row mt-3 items-center">
            <Text className="text-muted font-head text-xs mr-2">{t("subscription")}:</Text>
            <Text className={`font-head text-xs ${user.subscription_tier === "pro" ? "text-accent" : "text-muted"}`}>
              {user.subscription_tier === "pro" ? t("pro") : t("free")}
            </Text>
          </View>
          {user.subscription_tier !== "pro" && (
            <Pressable
              onPress={() => Linking.openURL(`${API_BASE_URL}/pricing`)}
              className="bg-accent rounded-full px-4 py-2 mt-4 self-start"
            >
              <Text className="text-black font-head text-xs">{t("manage_on_web")} ↗</Text>
            </Pressable>
          )}
        </Card>
      ) : (
        <Card>
          <Text className="text-text font-head">{t("no_account")}</Text>
          <Link href="/login" asChild>
            <Pressable className="bg-accent rounded-full px-4 py-2 mt-3 self-start">
              <Text className="text-black font-head">{t("login")}</Text>
            </Pressable>
          </Link>
        </Card>
      )}

      <SectionTitle>{t("language")}</SectionTitle>
      <View className="flex-row">
        <PillButton label={t("english")} active={lang === "en"} onPress={() => setLang("en")} />
        <PillButton label={t("chinese")} active={lang === "zh"} onPress={() => setLang("zh")} />
      </View>

      <SectionTitle>{t("more")}</SectionTitle>
      <Card>
        <MenuItem label={t("teams")} href="/teams" />
        <MenuItem label={t("players")} href="/players" />
        <MenuItem label={t("compare")} href="/compare" />
        <MenuItem label={t("news")} href="/news" />
        <MenuItem label={t("awards")} href="/awards" />
        <MenuItem label={t("draft")} href={`/draft/${new Date().getFullYear() - 1}`} last />
      </Card>

      {user && (
        <>
          <SectionTitle>{t("feedback")}</SectionTitle>
          <Card>
            <TextInput
              value={feedback}
              onChangeText={setFeedback}
              placeholder={t("feedback")}
              placeholderTextColor={colors.muted}
              multiline
              numberOfLines={3}
              className="bg-surface2 border border-border rounded-xl px-3 py-2 text-text font-sans min-h-20"
            />
            <Pressable onPress={sendFeedback} className="bg-surface2 border border-border rounded-full px-4 py-2 mt-3 self-start">
              <Text className="text-text font-head text-xs">{t("send_feedback")}</Text>
            </Pressable>
            {feedbackState === "sent" && <Text className="text-win font-sans text-xs mt-2">✓ Sent</Text>}
            {feedbackState === "error" && <Text className="text-loss font-sans text-xs mt-2">{t("error_generic")}</Text>}
          </Card>
        </>
      )}

      {user && (
        <Pressable
          onPress={async () => { await signOut(); router.replace("/(tabs)"); }}
          className="mt-6 self-start"
        >
          <Text className="text-loss font-head">{t("logout")}</Text>
        </Pressable>
      )}
    </Screen>
  );
}

function MenuItem({ label, href, last = false }: { label: string; href: any; last?: boolean }) {
  return (
    <Link href={href} asChild>
      <Pressable className={`flex-row items-center py-3 ${last ? "" : "border-b border-border/40"}`}>
        <Text className="flex-1 text-text font-head">{label}</Text>
        <Text className="text-muted font-head">›</Text>
      </Pressable>
    </Link>
  );
}
