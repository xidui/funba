import { useState } from "react";
import { Link } from "expo-router";
import { Pressable, Text, TextInput, View } from "react-native";
import { Screen } from "../components/Screen";
import { Card } from "../components/Card";
import { EmptyView, LoadingView } from "../components/LoadError";
import { usePlayerHints } from "../lib/queries";
import { colors } from "../lib/theme";
import { t } from "../lib/i18n";

export default function SearchScreen() {
  const [q, setQ] = useState("");
  const { data, isLoading } = usePlayerHints(q);

  return (
    <Screen scroll={false}>
      <TextInput
        value={q}
        onChangeText={setQ}
        placeholder={t("search_players")}
        placeholderTextColor={colors.muted}
        autoCapitalize="none"
        autoFocus
        className="bg-surface border border-border rounded-2xl px-4 py-3 text-text font-sans"
      />
      {q.length < 2 ? (
        <View className="py-10">
          <Text className="text-muted text-center font-sans">{t("search_players")}</Text>
        </View>
      ) : isLoading ? (
        <LoadingView />
      ) : (data?.players ?? []).length === 0 ? (
        <EmptyView />
      ) : (
        <View className="mt-3">
          {(data?.players ?? []).map((p: any) => (
            <Link href={`/players/${p.slug}`} asChild key={p.player_id}>
              <Pressable>
                <Card className="mb-2">
                  <View className="flex-row items-center">
                    <Text className="flex-1 text-text font-head">{p.display_name}</Text>
                    <Text className={`font-head text-[10px] uppercase ${p.is_active ? "text-accent" : "text-muted"}`}>
                      {p.is_active ? "Active" : "—"}
                    </Text>
                  </View>
                </Card>
              </Pressable>
            </Link>
          ))}
        </View>
      )}
    </Screen>
  );
}
