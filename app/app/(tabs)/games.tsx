import { useMemo, useState } from "react";
import { Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { PillButton, SectionTitle } from "../../components/Card";
import { GameRow } from "../../components/GameRow";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useGamesList } from "../../lib/queries";
import { useAppStore } from "../../lib/store";
import { formatDate } from "../../lib/format";
import { t } from "../../lib/i18n";

export default function GamesTab() {
  const [phase, setPhase] = useState<"2" | "4" | "5">("2");
  const [year, setYear] = useState<string | undefined>(undefined);
  const { data, isLoading, isError, refetch, isFetching } = useGamesList({ phase, year });
  const lang = useAppStore((s) => s.lang);

  const grouped = useMemo(() => {
    const map = new Map<string, any[]>();
    for (const g of data?.games ?? []) {
      const k = g.game_date;
      if (!map.has(k)) map.set(k, []);
      map.get(k)!.push(g);
    }
    return Array.from(map.entries());
  }, [data]);

  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("games")}</Text>

      <View className="flex-row flex-wrap">
        <PillButton label={t("regular_season")} active={phase === "2"} onPress={() => setPhase("2")} />
        <PillButton label={t("playoffs")} active={phase === "4"} onPress={() => setPhase("4")} />
        <PillButton label={t("play_in")} active={phase === "5"} onPress={() => setPhase("5")} />
      </View>

      {(data?.available_years ?? []).length > 0 && (
        <View className="flex-row flex-wrap mt-1">
          {(data?.available_years ?? []).slice(0, 8).map((y: string) => (
            <PillButton key={y} label={y} active={(year ?? data?.year) === y} onPress={() => setYear(y)} />
          ))}
        </View>
      )}

      {isLoading ? (
        <LoadingView />
      ) : isError ? (
        <ErrorView onRetry={refetch} />
      ) : grouped.length === 0 ? (
        <EmptyView />
      ) : (
        grouped.map(([date, games]) => (
          <View key={date}>
            <SectionTitle>{formatDate(date, lang)}</SectionTitle>
            {games.map((g) => (
              <GameRow key={g.game_id} game={g} />
            ))}
          </View>
        ))
      )}
    </Screen>
  );
}
