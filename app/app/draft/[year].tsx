import { useMemo, useState } from "react";
import { Link, useLocalSearchParams } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, PillButton, SectionTitle } from "../../components/Card";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useDraft } from "../../lib/queries";
import { t } from "../../lib/i18n";

export default function DraftScreen() {
  const params = useLocalSearchParams<{ year: string }>();
  const [year, setYear] = useState<string>(params.year ?? String(new Date().getFullYear() - 1));
  const { data, isLoading, isError, refetch, isFetching } = useDraft(year);

  const grouped = useMemo(() => {
    const map = new Map<number, any[]>();
    for (const p of data?.players ?? []) {
      const round = p.draft_round ?? 1;
      if (!map.has(round)) map.set(round, []);
      map.get(round)!.push(p);
    }
    return Array.from(map.entries()).sort((a, b) => a[0] - b[0]);
  }, [data]);

  const yearOptions: string[] = [];
  if (data?.max_year && data?.min_year) {
    for (let y = data.max_year; y >= Math.max(data.min_year, data.max_year - 20); y--) yearOptions.push(String(y));
  }

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("draft")} · {year}</Text>
      {yearOptions.length > 0 && (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mb-3">
          <View className="flex-row">
            {yearOptions.map((y) => (
              <PillButton key={y} label={y} active={year === y} onPress={() => setYear(y)} />
            ))}
          </View>
        </ScrollView>
      )}

      {isLoading ? <LoadingView /> : isError ? <ErrorView onRetry={refetch} /> : grouped.length === 0 ? <EmptyView /> : (
        grouped.map(([round, players]) => (
          <View key={round}>
            <SectionTitle>Round {round}</SectionTitle>
            <Card>
              {players.map((p: any) => (
                <Link href={`/players/${p.slug}`} asChild key={p.player_id}>
                  <Pressable>
                    <View className="flex-row items-center py-2 border-b border-border/40 last:border-b-0">
                      <Text className="text-muted font-head w-8 text-right">{p.draft_number}</Text>
                      <Text className="ml-3 flex-1 text-text font-head">{p.display_name}</Text>
                      <Text className="text-muted font-sans text-xs">{p.position ?? ""}</Text>
                    </View>
                  </Pressable>
                </Link>
              ))}
            </Card>
          </View>
        ))
      )}
    </Screen>
  );
}
