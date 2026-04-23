import { useMemo, useState } from "react";
import { Link, useLocalSearchParams } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, PillButton, SectionTitle, StatChip } from "../../components/Card";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useGameDetail } from "../../lib/queries";
import { plusMinusColor } from "../../lib/format";
import { t } from "../../lib/i18n";

export default function GameDetailScreen() {
  const { slug } = useLocalSearchParams<{ slug: string }>();
  const { data, isLoading, isError, refetch, isFetching } = useGameDetail(slug);
  const [tab, setTab] = useState<"box" | "pbp" | "metrics">("box");

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError || !data?.game) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const g = data.game;
  const winnerHome = g.wining_team_id === g.home_team?.team_id;
  const winnerRoad = g.wining_team_id === g.road_team?.team_id;

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Card className="mb-4">
        <View className="flex-row items-center justify-between">
          <View className="flex-1 items-center">
            <TeamBadge team={g.road_team} size="lg" />
            <Text className={`mt-2 font-head text-xs ${winnerRoad ? "text-text" : "text-muted"}`}>
              {g.road_team?.abbr}
            </Text>
            <Text className={`font-head text-3xl ${winnerRoad ? "text-text" : "text-muted"}`}>
              {g.road_team_score ?? "—"}
            </Text>
          </View>
          <View className="items-center">
            <Text className="text-muted font-head text-[10px] uppercase">{g.summary_text}</Text>
            <Text className="text-muted font-sans text-xs mt-1">{g.game_date}</Text>
            <Text className="text-muted font-sans text-[10px] mt-1">{g.season_label}</Text>
          </View>
          <View className="flex-1 items-center">
            <TeamBadge team={g.home_team} size="lg" />
            <Text className={`mt-2 font-head text-xs ${winnerHome ? "text-text" : "text-muted"}`}>
              {g.home_team?.abbr}
            </Text>
            <Text className={`font-head text-3xl ${winnerHome ? "text-text" : "text-muted"}`}>
              {g.home_team_score ?? "—"}
            </Text>
          </View>
        </View>
      </Card>

      {(data.quarter_scores ?? []).length > 0 && (
        <Card className="mb-4">
          <View className="flex-row">
            <Text className="w-10" />
            {data.quarter_scores.map((q: any) => (
              <Text key={q.period} className="flex-1 text-center text-muted font-head text-xs">{q.label}</Text>
            ))}
            <Text className="w-10 text-right text-muted font-head text-xs">T</Text>
          </View>
          <View className="flex-row mt-2">
            <Text className="w-10 text-text font-head text-xs">{g.road_team?.abbr}</Text>
            {data.quarter_scores.map((q: any) => (
              <Text key={q.period} className="flex-1 text-center text-text font-sans">{q.road}</Text>
            ))}
            <Text className="w-10 text-right text-text font-head">{g.road_team_score}</Text>
          </View>
          <View className="flex-row mt-1">
            <Text className="w-10 text-text font-head text-xs">{g.home_team?.abbr}</Text>
            {data.quarter_scores.map((q: any) => (
              <Text key={q.period} className="flex-1 text-center text-text font-sans">{q.home}</Text>
            ))}
            <Text className="w-10 text-right text-text font-head">{g.home_team_score}</Text>
          </View>
        </Card>
      )}

      <View className="flex-row mb-3">
        <PillButton label={t("box_score")} active={tab === "box"} onPress={() => setTab("box")} />
        <PillButton label={t("play_by_play")} active={tab === "pbp"} onPress={() => setTab("pbp")} />
        <PillButton label={t("metrics")} active={tab === "metrics"} onPress={() => setTab("metrics")} />
      </View>

      {tab === "box" && (
        <BoxScoreTab data={data} />
      )}
      {tab === "pbp" && (
        <PbpTab data={data} />
      )}
      {tab === "metrics" && (
        <MetricsTab data={data} />
      )}
    </Screen>
  );
}

function BoxScoreTab({ data }: { data: any }) {
  const g = data.game;
  const ordered = data.ordered_team_ids ?? [];
  return (
    <View>
      {(data.team_stats ?? []).map((ts: any) => (
        <Card key={ts.team_id} className="mb-3">
          <View className="flex-row items-center mb-2">
            <TeamBadge team={ts.team} />
            <Text className="ml-3 text-text font-head text-lg flex-1">{ts.team?.display_name ?? ts.team_id}</Text>
            <Text className="text-text font-head text-lg">{ts.pts}</Text>
          </View>
          <ScrollView horizontal showsHorizontalScrollIndicator={false}>
            <View className="flex-row">
              <StatChip label="FG" value={`${ts.fgm}/${ts.fga}`} />
              <StatChip label="3P" value={`${ts.fg3m}/${ts.fg3a}`} />
              <StatChip label="FT" value={`${ts.ftm}/${ts.fta}`} />
              <StatChip label="REB" value={ts.reb ?? "-"} />
              <StatChip label="AST" value={ts.ast ?? "-"} />
              <StatChip label="STL" value={ts.stl ?? "-"} />
              <StatChip label="BLK" value={ts.blk ?? "-"} />
              <StatChip label="TOV" value={ts.tov ?? "-"} />
            </View>
          </ScrollView>
        </Card>
      ))}

      {ordered.map((tid: string) => {
        const rows = (data.players_by_team ?? {})[tid] ?? [];
        const teamObj = tid === g.home_team?.team_id ? g.home_team : g.road_team;
        return (
          <Card key={tid} className="mb-3">
            <View className="flex-row items-center mb-2">
              <TeamBadge team={teamObj} size="sm" />
              <Text className="ml-2 text-text font-head">{teamObj?.display_name ?? tid}</Text>
            </View>
            <PlayerBoxTable rows={rows} />
          </Card>
        );
      })}
    </View>
  );
}

function PlayerBoxTable({ rows }: { rows: any[] }) {
  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false}>
      <View>
        <View className="flex-row border-b border-border pb-1">
          <HeaderCell label="" w={120} />
          <HeaderCell label={t("minutes")} w={60} />
          <HeaderCell label={t("pts")} w={44} />
          <HeaderCell label={t("reb")} w={44} />
          <HeaderCell label={t("ast")} w={44} />
          <HeaderCell label={t("stl")} w={44} />
          <HeaderCell label={t("blk")} w={44} />
          <HeaderCell label={t("tov")} w={44} />
          <HeaderCell label={t("plus_minus")} w={52} />
        </View>
        {rows.map((r) => (
          <Link href={`/players/${r.player?.slug ?? r.player_id}`} asChild key={r.player_id}>
            <Pressable className="flex-row items-center py-1.5 border-b border-border/40">
              <DataCell label={(r.is_starter ? "★ " : "  ") + (r.player?.display_name ?? r.player_id)} w={120} bold={r.is_starter} muted={r.is_dnp} />
              <DataCell label={r.is_dnp ? "DNP" : r.minutes_display} w={60} muted={r.is_dnp} />
              <DataCell label={r.pts} w={44} />
              <DataCell label={r.reb} w={44} />
              <DataCell label={r.ast} w={44} />
              <DataCell label={r.stl} w={44} />
              <DataCell label={r.blk} w={44} />
              <DataCell label={r.tov} w={44} />
              <Text style={{ width: 52 }} className={`text-right font-sans text-xs ${plusMinusColor(r.plus)}`}>
                {r.plus === 0 ? "0" : (r.plus > 0 ? `+${r.plus}` : r.plus)}
              </Text>
            </Pressable>
          </Link>
        ))}
      </View>
    </ScrollView>
  );
}

function HeaderCell({ label, w }: { label: string; w: number }) {
  return <Text style={{ width: w }} className="text-muted font-head text-[10px] uppercase text-right">{label}</Text>;
}

function DataCell({ label, w, bold = false, muted = false }: { label: any; w: number; bold?: boolean; muted?: boolean }) {
  return (
    <Text
      style={{ width: w }}
      className={`font-sans text-xs text-right ${bold ? "font-head" : ""} ${muted ? "text-muted" : "text-text"}`}
      numberOfLines={1}
    >
      {label ?? "-"}
    </Text>
  );
}

function PbpTab({ data }: { data: any }) {
  const rows = data.pbp ?? [];
  const grouped = useMemo(() => {
    const map = new Map<number, any[]>();
    for (const r of rows) {
      const k = r.period ?? 0;
      if (!map.has(k)) map.set(k, []);
      map.get(k)!.push(r);
    }
    return Array.from(map.entries()).sort((a, b) => a[0] - b[0]);
  }, [rows]);

  if (rows.length === 0) return <EmptyView />;
  return (
    <View>
      {grouped.map(([period, events]) => (
        <View key={period} className="mb-3">
          <Text className="text-accent font-head text-xs mb-1">Q{period}</Text>
          <Card>
            {events.map((e: any, i: number) => (
              <View key={i} className="flex-row py-1 border-b border-border/40 last:border-b-0">
                <Text className="text-muted font-head text-xs w-14">{e.clock}</Text>
                <Text className="flex-1 text-text font-sans text-xs">{e.description}</Text>
                <Text className="text-muted font-head text-xs w-14 text-right">{e.score}</Text>
              </View>
            ))}
          </Card>
        </View>
      ))}
    </View>
  );
}

function MetricsTab({ data }: { data: any }) {
  const metrics = data.metrics ?? [];
  if (metrics.length === 0) return <EmptyView />;
  return (
    <View>
      {metrics.map((m: any, i: number) => (
        <Link href={`/metrics/${m.metric_key}`} asChild key={`${m.metric_key}-${i}`}>
          <Pressable>
            <Card className="mb-2">
              <View className="flex-row justify-between items-center">
                <Text className="flex-1 text-text font-head">{m.metric_name}</Text>
                <Text className="text-accent font-head">{m.value_str ?? m.value_num}</Text>
              </View>
              {m.is_notable && (
                <Text className="text-accent-soft font-sans text-xs mt-1">★ {t("notable")}{m.notable_reason ? ` — ${m.notable_reason}` : ""}</Text>
              )}
            </Card>
          </Pressable>
        </Link>
      ))}
    </View>
  );
}
