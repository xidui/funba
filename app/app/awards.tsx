import { useState } from "react";
import { Link } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Screen } from "../components/Screen";
import { Card, PillButton } from "../components/Card";
import { TeamBadge } from "../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../components/LoadError";
import { useAwards } from "../lib/queries";
import { t } from "../lib/i18n";

export default function AwardsScreen() {
  const [type, setType] = useState<string>("mvp");
  const { data, isLoading, isError, refetch, isFetching } = useAwards(type);

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("awards")}</Text>
      <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mb-3">
        <View className="flex-row">
          {(data?.available_types ?? []).map((a: any) => (
            <PillButton key={a.code} label={a.label} active={type === a.code} onPress={() => setType(a.code)} />
          ))}
        </View>
      </ScrollView>

      {isLoading ? <LoadingView /> : isError ? <ErrorView onRetry={refetch} /> : (data?.results ?? []).length === 0 ? <EmptyView /> : (
        <View>
          {data.results.map((row: any) => (
            <AwardRow key={row.id} row={row} />
          ))}
        </View>
      )}
    </Screen>
  );
}

function AwardRow({ row }: { row: any }) {
  const body = (
    <Card className="mb-2">
      <View className="flex-row items-center">
        <Text className="text-accent-soft font-head w-16">{row.season}</Text>
        {row.team ? <TeamBadge team={row.team} size="sm" /> : <View className="w-8" />}
        <Text className="ml-3 flex-1 text-text font-head">
          {row.player?.display_name ?? row.team?.display_name ?? "—"}
        </Text>
      </View>
      {row.notes ? <Text className="text-muted font-sans text-xs mt-1">{row.notes}</Text> : null}
    </Card>
  );
  const href = row.player ? `/players/${row.player.slug}` : row.team ? `/teams/${row.team.slug}` : undefined;
  if (!href) return body;
  return <Link href={href} asChild><Pressable>{body}</Pressable></Link>;
}
