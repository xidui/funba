import { useState } from "react";
import { Link, useLocalSearchParams } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, SectionTitle, StatChip, PillButton } from "../../components/Card";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useTeamDetail } from "../../lib/queries";
import { num, resultColor } from "../../lib/format";
import { t } from "../../lib/i18n";

export default function TeamDetailScreen() {
  const { slug } = useLocalSearchParams<{ slug: string }>();
  const [season, setSeason] = useState<string | undefined>(undefined);
  const { data, isLoading, isError, refetch, isFetching } = useTeamDetail(slug, season);
  const [tab, setTab] = useState<"games" | "roster" | "totals">("games");

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError || !data?.team) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const team = data.team;
  const rec = data.record ?? { wins: 0, losses: 0, win_pct: 0 };
  const totals = data.totals ?? {};

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Card className="mb-4">
        <View className="flex-row items-center">
          <TeamBadge team={team} size="lg" />
          <View className="ml-4 flex-1">
            <Text className="text-text font-head text-2xl">{team.display_name}</Text>
            <Text className="text-muted font-sans text-xs">{team.city}</Text>
          </View>
          <View className="items-end">
            <Text className="text-text font-head text-lg">{rec.wins}-{rec.losses}</Text>
            <Text className="text-muted font-sans text-xs">{(rec.win_pct * 100).toFixed(1)}%</Text>
          </View>
        </View>
      </Card>

      {(data.season_options ?? []).length > 0 && (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mb-3">
          <View className="flex-row">
            {data.season_options.slice(0, 10).map((s: any) => (
              <PillButton key={s.season} label={s.season.slice(1)} active={(season ?? data.selected_season) === s.season} onPress={() => setSeason(s.season)} />
            ))}
          </View>
        </ScrollView>
      )}

      <View className="flex-row mb-3">
        <PillButton label={t("game_log")} active={tab === "games"} onPress={() => setTab("games")} />
        <PillButton label={t("roster")} active={tab === "roster"} onPress={() => setTab("roster")} />
        <PillButton label={t("totals")} active={tab === "totals"} onPress={() => setTab("totals")} />
      </View>

      {tab === "games" && <TeamGames games={data.games ?? []} />}
      {tab === "roster" && <Roster roster={data.roster ?? []} coaches={data.coaches ?? []} />}
      {tab === "totals" && <Totals totals={totals} />}
    </Screen>
  );
}

function TeamGames({ games }: { games: any[] }) {
  if (games.length === 0) return <EmptyView />;
  return (
    <View>
      {games.map((g: any) => (
        <Link href={`/games/${g.slug}`} asChild key={g.game_id}>
          <Pressable>
            <Card className={`mb-2 ${g.result === "W" ? "border-l-4 border-l-win" : g.result === "L" ? "border-l-4 border-l-loss" : ""}`}>
              <View className="flex-row items-center">
                <Text className="text-muted font-sans text-xs w-24">{g.game_date}</Text>
                <Text className={`font-head text-xs w-6 ${resultColor(g.result)}`}>{g.result}</Text>
                <Text className="flex-1 text-text font-sans text-xs">
                  {g.is_home ? "vs " : "@ "}{g.opponent?.abbr ?? "-"}
                </Text>
                <Text className="text-text font-head text-sm">
                  {g.my_score ?? "-"}–{g.opp_score ?? "-"}
                </Text>
              </View>
            </Card>
          </Pressable>
        </Link>
      ))}
    </View>
  );
}

function Roster({ roster, coaches }: { roster: any[]; coaches: any[] }) {
  return (
    <View>
      <SectionTitle>{t("roster")}</SectionTitle>
      <Card>
        {roster.length === 0 ? <EmptyView /> : roster.map((p: any) => (
          <Link href={`/players/${p.slug}`} asChild key={p.player_id}>
            <Pressable className="flex-row items-center py-2 border-b border-border/40 last:border-b-0">
              <Text className="text-muted font-head w-8 text-xs">#{p.jersey ?? "—"}</Text>
              <Text className="flex-1 text-text font-head">{p.display_name}</Text>
              <Text className="text-muted font-sans text-xs">{p.position ?? "—"}</Text>
            </Pressable>
          </Link>
        ))}
      </Card>

      <SectionTitle>{t("coaches")}</SectionTitle>
      <Card>
        {coaches.length === 0 ? <EmptyView /> : coaches.map((c: any, i: number) => (
          <View key={i} className="flex-row py-2 border-b border-border/40 last:border-b-0">
            <Text className="flex-1 text-text font-head">{c.coach_name}</Text>
            <Text className="text-muted font-sans text-xs">{c.coach_type ?? (c.is_assistant ? "Assistant" : "Head")}</Text>
          </View>
        ))}
      </Card>
    </View>
  );
}

function Totals({ totals }: { totals: any }) {
  if (!totals?.games) return <EmptyView />;
  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false}>
      <View className="flex-row">
        <StatChip label="GP" value={totals.games} />
        <StatChip label="PPG" value={num(totals.ppg)} />
        <StatChip label="FG%" value={totals.fg_pct != null ? `${(totals.fg_pct * 100).toFixed(1)}` : "-"} />
        <StatChip label="3P%" value={totals.fg3_pct != null ? `${(totals.fg3_pct * 100).toFixed(1)}` : "-"} />
        <StatChip label="FT%" value={totals.ft_pct != null ? `${(totals.ft_pct * 100).toFixed(1)}` : "-"} />
        <StatChip label="RPG" value={num(totals.reb_pg)} />
        <StatChip label="APG" value={num(totals.ast_pg)} />
        <StatChip label="SPG" value={num(totals.stl_pg)} />
        <StatChip label="BPG" value={num(totals.blk_pg)} />
      </View>
    </ScrollView>
  );
}
