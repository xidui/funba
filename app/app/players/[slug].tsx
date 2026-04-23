import { useState } from "react";
import { Link, useLocalSearchParams } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, SectionTitle, StatChip, PillButton } from "../../components/Card";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { usePlayerDetail } from "../../lib/queries";
import { num, resultColor, plusMinusColor } from "../../lib/format";
import { t } from "../../lib/i18n";
import { ShotHeatmap } from "../../components/ShotHeatmap";

export default function PlayerDetailScreen() {
  const { slug } = useLocalSearchParams<{ slug: string }>();
  const [season, setSeason] = useState<string | undefined>(undefined);
  const { data, isLoading, isError, refetch, isFetching } = usePlayerDetail(slug, season);
  const [view, setView] = useState<"per_game" | "totals" | "game_log" | "shots">("per_game");

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError || !data?.player) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const p = data.player;
  const career = data.career_overall ?? {};

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Card className="mb-4">
        <Text className="text-text font-head text-2xl">{p.display_name}</Text>
        <Text className="text-muted font-sans text-xs mt-1">
          {[p.position, p.jersey && `#${p.jersey}`, p.height, p.weight && `${p.weight} lbs`].filter(Boolean).join("  ·  ")}
        </Text>
        <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mt-3">
          <View className="flex-row">
            <StatChip label="PPG" value={num(career.ppg)} />
            <StatChip label="RPG" value={num(career.rpg)} />
            <StatChip label="APG" value={num(career.apg)} />
            <StatChip label="MPG" value={num(career.mpg)} />
            <StatChip label="GP" value={career.games ?? 0} />
          </View>
        </ScrollView>
      </Card>

      {(data.awards ?? []).length > 0 && (
        <View className="mb-4">
          <ScrollView horizontal showsHorizontalScrollIndicator={false}>
            <View className="flex-row">
              {data.awards.map((a: any) => (
                <View key={a.award_type} className="bg-surface2 rounded-xl px-3 py-2 mr-2">
                  <Text className="text-accent-soft font-head text-xs">★ {a.award_type.toUpperCase()}</Text>
                  <Text className="text-muted font-sans text-[10px]">×{a.count}</Text>
                </View>
              ))}
            </View>
          </ScrollView>
        </View>
      )}

      <View className="flex-row flex-wrap">
        <PillButton label={t("by_season")} active={view === "per_game"} onPress={() => setView("per_game")} />
        <PillButton label={t("totals")} active={view === "totals"} onPress={() => setView("totals")} />
        <PillButton label={t("game_log")} active={view === "game_log"} onPress={() => setView("game_log")} />
        <PillButton label={t("shot_chart")} active={view === "shots"} onPress={() => setView("shots")} />
      </View>

      {(data.season_options ?? []).length > 0 && (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mt-1">
          <View className="flex-row">
            {data.season_options.slice(0, 12).map((s: string) => (
              <PillButton key={s} label={s} active={(season ?? data.selected_season) === s} onPress={() => setSeason(s)} />
            ))}
          </View>
        </ScrollView>
      )}

      {view === "per_game" && <CareerTable rows={data.career_season_rows ?? []} mode="per_game" />}
      {view === "totals" && <CareerTable rows={data.career_season_rows ?? []} mode="totals" />}
      {view === "game_log" && <GameLog rows={data.game_log ?? []} />}
      {view === "shots" && <ShotsView heatmap={data.heatmap ?? {}} />}
    </Screen>
  );
}

function CareerTable({ rows, mode }: { rows: any[]; mode: "per_game" | "totals" }) {
  if (rows.length === 0) return <EmptyView />;
  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false}>
      <View className="mt-2">
        <View className="flex-row border-b border-border pb-1">
          <HeaderCell label="Season" w={96} />
          <HeaderCell label="GP" w={40} />
          <HeaderCell label={mode === "per_game" ? "PPG" : "PTS"} w={52} />
          <HeaderCell label={mode === "per_game" ? "RPG" : "REB"} w={52} />
          <HeaderCell label={mode === "per_game" ? "APG" : "AST"} w={52} />
          <HeaderCell label="FG%" w={52} />
          <HeaderCell label="3P%" w={52} />
          <HeaderCell label="FT%" w={52} />
        </View>
        {rows.map((r: any) => {
          const pts = mode === "per_game" ? num(r.ppg) : r.pts;
          const reb = mode === "per_game" ? num(r.rpg) : r.reb;
          const ast = mode === "per_game" ? num(r.apg) : r.ast;
          return (
            <View key={r.season} className="flex-row py-1 border-b border-border/40">
              <Text style={{ width: 96 }} className="text-text font-sans text-xs" numberOfLines={1}>{r.season_label}</Text>
              <Text style={{ width: 40 }} className="text-text font-sans text-xs text-right">{r.games}</Text>
              <Text style={{ width: 52 }} className="text-text font-sans text-xs text-right">{pts}</Text>
              <Text style={{ width: 52 }} className="text-text font-sans text-xs text-right">{reb}</Text>
              <Text style={{ width: 52 }} className="text-text font-sans text-xs text-right">{ast}</Text>
              <Text style={{ width: 52 }} className="text-text font-sans text-xs text-right">{r.fg_pct != null ? `${(r.fg_pct * 100).toFixed(1)}` : "-"}</Text>
              <Text style={{ width: 52 }} className="text-text font-sans text-xs text-right">{r.fg3_pct != null ? `${(r.fg3_pct * 100).toFixed(1)}` : "-"}</Text>
              <Text style={{ width: 52 }} className="text-text font-sans text-xs text-right">{r.ft_pct != null ? `${(r.ft_pct * 100).toFixed(1)}` : "-"}</Text>
            </View>
          );
        })}
      </View>
    </ScrollView>
  );
}

function GameLog({ rows }: { rows: any[] }) {
  if (rows.length === 0) return <EmptyView />;
  return (
    <View className="mt-2">
      {rows.map((r: any) => (
        <Link href={`/games/${r.slug}`} asChild key={r.game_id}>
          <Pressable>
            <Card className={`mb-2 ${r.result === "W" ? "border-l-4 border-l-win" : r.result === "L" ? "border-l-4 border-l-loss" : ""}`}>
              <View className="flex-row items-center">
                <Text className="text-muted font-sans text-xs w-24">{r.game_date}</Text>
                <Text className={`font-head text-xs w-6 ${resultColor(r.result)}`}>{r.result}</Text>
                <Text className="text-text font-sans text-xs flex-1">
                  {r.is_home ? "vs " : "@ "}{r.opponent?.abbr ?? "-"}   {r.my_score ?? "-"}–{r.opp_score ?? "-"}
                </Text>
                <Text className="text-text font-head text-sm">{r.pts}p</Text>
                <Text className="text-muted font-sans text-xs ml-2">{r.reb}r {r.ast}a</Text>
                <Text className={`ml-2 text-xs font-sans ${plusMinusColor(r.plus)}`}>
                  {r.plus > 0 ? `+${r.plus}` : r.plus}
                </Text>
              </View>
            </Card>
          </Pressable>
        </Link>
      ))}
    </View>
  );
}

function ShotsView({ heatmap }: { heatmap: any }) {
  if (!heatmap?.dots?.length) return <EmptyView />;
  return (
    <View className="mt-2">
      <ShotHeatmap dots={heatmap.dots} zones={heatmap.zones ?? {}} />
      <Card className="mt-3">
        {Object.entries(heatmap.zones ?? {}).map(([zone, v]: [string, any]) => (
          <View key={zone} className="flex-row py-1 border-b border-border/40 last:border-b-0">
            <Text className="flex-1 text-text font-sans text-xs">{zone}</Text>
            <Text className="text-muted font-sans text-xs w-20 text-right">{v.made}/{v.attempts}</Text>
            <Text className="text-accent font-head text-xs w-16 text-right">{v.pct != null ? `${(v.pct * 100).toFixed(1)}%` : "-"}</Text>
          </View>
        ))}
      </Card>
    </View>
  );
}

function HeaderCell({ label, w }: { label: string; w: number }) {
  return <Text style={{ width: w }} className="text-muted font-head text-[10px] uppercase text-right">{label}</Text>;
}
