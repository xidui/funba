import { Text, View } from "react-native";

type Team = {
  abbr?: string | null;
  display_name?: string | null;
  full_name?: string | null;
} | null | undefined;

export function TeamBadge({ team, size = "md" }: { team: Team; size?: "sm" | "md" | "lg" }) {
  const abbr = team?.abbr ?? "—";
  const clsByBox = { sm: "w-8 h-8", md: "w-10 h-10", lg: "w-14 h-14" }[size];
  const clsByText = { sm: "text-[10px]", md: "text-xs", lg: "text-base" }[size];
  return (
    <View className={`bg-surface2 rounded-lg items-center justify-center ${clsByBox}`}>
      <Text className={`text-text font-head ${clsByText}`}>{abbr}</Text>
    </View>
  );
}
