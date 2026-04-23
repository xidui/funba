import { useState } from "react";
import { Link } from "expo-router";
import { Pressable, Text, TextInput, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, PillButton, SectionTitle } from "../../components/Card";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useMetricsBrowse } from "../../lib/queries";
import { colors } from "../../lib/theme";
import { t } from "../../lib/i18n";

type Scope = "all" | "player" | "team" | "game";

export default function MetricsTab() {
  const [scope, setScope] = useState<Scope>("all");
  const [q, setQ] = useState("");
  const { data, isLoading, isError, refetch, isFetching } = useMetricsBrowse({
    scope: scope === "all" ? undefined : scope,
    q,
  });

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("metrics")}</Text>
      <TextInput
        value={q}
        onChangeText={setQ}
        placeholder="Search metrics"
        placeholderTextColor={colors.muted}
        autoCapitalize="none"
        className="bg-surface border border-border rounded-2xl px-4 py-3 text-text font-sans mb-3"
      />
      <View className="flex-row">
        <PillButton label={t("all")} active={scope === "all"} onPress={() => setScope("all")} />
        <PillButton label="Player" active={scope === "player"} onPress={() => setScope("player")} />
        <PillButton label="Team" active={scope === "team"} onPress={() => setScope("team")} />
        <PillButton label="Game" active={scope === "game"} onPress={() => setScope("game")} />
      </View>

      <Link href="/metrics/mine" asChild>
        <Pressable className="bg-surface border border-border rounded-xl py-2 px-3 mt-2 self-start">
          <Text className="text-accent font-head text-xs">{t("my_metrics")} →</Text>
        </Pressable>
      </Link>

      {isLoading ? <LoadingView /> : isError ? <ErrorView onRetry={refetch} /> : (data?.metrics ?? []).length === 0 ? <EmptyView /> : (
        <View className="mt-3">
          {data.metrics.map((m: any) => (
            <Link href={`/metrics/${m.key}`} asChild key={m.key}>
              <Pressable>
                <Card className="mb-2">
                  <Text className="text-text font-head">{m.name}</Text>
                  {m.description ? <Text className="text-muted font-sans text-xs mt-1" numberOfLines={2}>{m.description}</Text> : null}
                  <Text className="text-accent font-head text-[10px] uppercase mt-1">{m.scope}</Text>
                </Card>
              </Pressable>
            </Link>
          ))}
        </View>
      )}
    </Screen>
  );
}
