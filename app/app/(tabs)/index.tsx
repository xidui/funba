import { useState } from "react";
import { Link } from "expo-router";
import { Pressable, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, PillButton, SectionTitle } from "../../components/Card";
import { GameRow } from "../../components/GameRow";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useHome } from "../../lib/queries";
import { t } from "../../lib/i18n";

export default function HomeTab() {
  const [conf, setConf] = useState<"east" | "west">("east");
  const { data, isLoading, isError, refetch, isFetching } = useHome();

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError || !data) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const standings = data[`${conf}_standings`] ?? [];
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-3xl mb-1">Funba</Text>
      <Text className="text-muted font-sans text-xs mb-4">{data.season_label}</Text>

      <SectionTitle>{t("standings")}</SectionTitle>
      <View className="flex-row mb-2">
        <PillButton label={t("east")} active={conf === "east"} onPress={() => setConf("east")} />
        <PillButton label={t("west")} active={conf === "west"} onPress={() => setConf("west")} />
      </View>
      <Card>
        {standings.length === 0 ? <EmptyView /> : standings.map((row: any, idx: number) => (
          <Link href={`/teams/${row.team.slug}`} asChild key={row.team_id}>
            <Pressable>
              <View className="flex-row items-center py-2 border-b border-border last:border-b-0">
                <Text className="text-muted font-head w-6">{idx + 1}</Text>
                <TeamBadge team={row.team} size="sm" />
                <Text className="ml-3 flex-1 text-text font-head">{row.team.display_name}</Text>
                <Text className="text-text font-head w-20 text-right">{row.wins}-{row.losses}</Text>
                <Text className="text-muted font-sans w-14 text-right">{(row.win_pct * 100).toFixed(1)}%</Text>
              </View>
            </Pressable>
          </Link>
        ))}
      </Card>

      <SectionTitle
        right={
          <Link href="/(tabs)/games" className="text-accent font-head text-sm">{t("more")} →</Link>
        }
      >{t("recent_games")}</SectionTitle>
      {(data.recent_games ?? []).map((g: any) => (<GameRow key={g.game_id} game={g} />))}

      <SectionTitle
        right={
          <Link href="/teams" className="text-accent font-head text-sm">{t("more")} →</Link>
        }
      >{t("teams")}</SectionTitle>
      <Card>
        <View className="flex-row flex-wrap">
          {(data.teams ?? []).slice(0, 24).map((team: any) => (
            <Link href={`/teams/${team.slug}`} asChild key={team.team_id}>
              <Pressable className="w-1/4 py-2 items-center">
                <TeamBadge team={team} />
                <Text className="text-muted text-[10px] font-sans mt-1">{team.abbr}</Text>
              </Pressable>
            </Link>
          ))}
        </View>
      </Card>

      <View className="flex-row mt-6">
        <Link href="/search" asChild>
          <Pressable className="flex-1 bg-surface border border-border rounded-xl py-3 mr-2 items-center">
            <Text className="text-text font-head">{t("search_players")}</Text>
          </Pressable>
        </Link>
        <Link href="/news" asChild>
          <Pressable className="flex-1 bg-surface border border-border rounded-xl py-3 items-center">
            <Text className="text-text font-head">{t("news")}</Text>
          </Pressable>
        </Link>
      </View>
    </Screen>
  );
}
