import { useState } from "react";
import { Link, useLocalSearchParams } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, PillButton } from "../../components/Card";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useMetricDetail } from "../../lib/queries";
import { t } from "../../lib/i18n";

export default function MetricDetailScreen() {
  const { key } = useLocalSearchParams<{ key: string }>();
  const [season, setSeason] = useState<string | undefined>(undefined);
  const [page, setPage] = useState(1);
  const { data, isLoading, isError, refetch, isFetching } = useMetricDetail(key, { season, page });

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError || !data?.metric) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const m = data.metric;
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-1">{m.name}</Text>
      {m.description ? <Text className="text-muted font-sans text-xs mb-3">{m.description}</Text> : null}

      {(data.season_options ?? []).length > 0 && (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mb-3">
          <View className="flex-row">
            {data.season_options.slice(0, 12).map((s: string) => (
              <PillButton key={s} label={s} active={(season ?? data.selected_season) === s} onPress={() => { setSeason(s); setPage(1); }} />
            ))}
          </View>
        </ScrollView>
      )}

      {(data.results ?? []).length === 0 ? <EmptyView /> : (
        <View>
          {data.results.map((r: any) => (
            <EntityRow key={`${r.entity_type}-${r.entity_id}-${r.season}-${r.rank}`} row={r} />
          ))}
        </View>
      )}

      {data.total_pages > 1 && (
        <View className="flex-row justify-between mt-4">
          <Pressable onPress={() => setPage((p) => Math.max(1, p - 1))} className="bg-surface border border-border rounded-full px-4 py-2">
            <Text className="text-text font-head">← Prev</Text>
          </Pressable>
          <Text className="text-muted font-sans self-center">{data.page} / {data.total_pages}</Text>
          <Pressable onPress={() => setPage((p) => Math.min(data.total_pages, p + 1))} className="bg-surface border border-border rounded-full px-4 py-2">
            <Text className="text-text font-head">Next →</Text>
          </Pressable>
        </View>
      )}
    </Screen>
  );
}

function EntityRow({ row }: { row: any }) {
  const href =
    row.entity_type === "player" && row.entity?.slug ? `/players/${row.entity.slug}` :
    row.entity_type === "team" && row.entity?.slug ? `/teams/${row.entity.slug}` : undefined;
  const body = (
    <Card className="mb-2">
      <View className="flex-row items-center">
        <Text className="text-muted font-head w-8">{row.rank}</Text>
        <View className="flex-1">
          <Text className="text-text font-head">{row.entity_label}</Text>
          {row.season_label ? <Text className="text-muted font-sans text-[10px]">{row.season_label}</Text> : null}
        </View>
        <Text className="text-accent font-head text-lg">{row.value_str ?? row.value_num}</Text>
      </View>
      {row.is_notable && (
        <Text className="text-accent-soft font-sans text-xs mt-1">★ {t("notable")}{row.notable_reason ? ` — ${row.notable_reason}` : ""}</Text>
      )}
    </Card>
  );
  if (!href) return body;
  return <Link href={href} asChild><Pressable>{body}</Pressable></Link>;
}
