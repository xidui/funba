import { Link } from "expo-router";
import { View, Text, Pressable } from "react-native";
import { TeamBadge } from "./TeamBadge";
import { formatDate } from "../lib/format";
import { useAppStore } from "../lib/store";

type GameRowProps = {
  game: any;
};

export function GameRow({ game }: GameRowProps) {
  const lang = useAppStore((s) => s.lang);
  const winnerHome = game.wining_team_id === game.home_team?.team_id;
  const winnerRoad = game.wining_team_id === game.road_team?.team_id;
  return (
    <Link href={`/games/${game.slug ?? game.game_id}`} asChild>
      <Pressable>
        <View className="bg-surface rounded-2xl p-3 border border-border mb-3">
          <View className="flex-row items-center justify-between mb-1">
            <Text className="text-muted font-sans text-xs">{formatDate(game.game_date, lang)}</Text>
            <Text className={`font-head text-[10px] uppercase ${game.status === "live" ? "text-accent" : "text-muted"}`}>
              {game.status === "live" ? "LIVE" : game.status === "upcoming" ? "UP" : game.summary_text ?? "FINAL"}
            </Text>
          </View>
          <View className="flex-row items-center">
            <TeamBadge team={game.road_team} />
            <Text className={`ml-3 flex-1 font-head ${winnerRoad ? "text-text" : "text-muted"}`}>
              {game.road_team?.display_name ?? "—"}
            </Text>
            <Text className={`font-head text-lg ${winnerRoad ? "text-text" : "text-muted"}`}>
              {game.road_team_score ?? "—"}
            </Text>
          </View>
          <View className="flex-row items-center mt-2">
            <TeamBadge team={game.home_team} />
            <Text className={`ml-3 flex-1 font-head ${winnerHome ? "text-text" : "text-muted"}`}>
              {game.home_team?.display_name ?? "—"}
            </Text>
            <Text className={`font-head text-lg ${winnerHome ? "text-text" : "text-muted"}`}>
              {game.home_team_score ?? "—"}
            </Text>
          </View>
        </View>
      </Pressable>
    </Link>
  );
}
