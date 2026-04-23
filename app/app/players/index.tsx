import { Link } from "expo-router";
import { Pressable, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card } from "../../components/Card";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { usePlayersBrowse } from "../../lib/queries";
import { num } from "../../lib/format";
import { t } from "../../lib/i18n";

export default function PlayersIndex() {
  const { data, isLoading, isError, refetch, isFetching } = usePlayersBrowse({});
  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError) return <Screen><ErrorView onRetry={refetch} /></Screen>;
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("players")}</Text>
      {(data?.players ?? []).length === 0 ? <EmptyView /> : (
        <Card>
          {data.players.slice(0, 200).map((p: any) => (
            <Link href={`/players/${p.slug}`} asChild key={p.player_id}>
              <Pressable>
                <View className="flex-row items-center py-2 border-b border-border/40 last:border-b-0">
                  <TeamBadge team={p.team} size="sm" />
                  <Text className="ml-3 flex-1 text-text font-head">{p.display_name}</Text>
                  <Text className="text-text font-head text-sm w-14 text-right">{num(p.summary?.ppg)}</Text>
                  <Text className="text-muted font-sans text-xs w-8 text-right">p</Text>
                </View>
              </Pressable>
            </Link>
          ))}
        </Card>
      )}
    </Screen>
  );
}
