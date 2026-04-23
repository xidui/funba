import { Link } from "expo-router";
import { Pressable, Text, View } from "react-native";
import { Screen } from "../../components/Screen";
import { Card } from "../../components/Card";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useTeamsList } from "../../lib/queries";
import { t } from "../../lib/i18n";

export default function TeamsIndex() {
  const { data, isLoading, isError, refetch, isFetching } = useTeamsList();
  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError) return <Screen><ErrorView onRetry={refetch} /></Screen>;
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("teams")}</Text>
      {(data?.teams ?? []).length === 0 ? <EmptyView /> : (
        <Card>
          {data.teams.map((team: any) => (
            <Link href={`/teams/${team.slug}`} asChild key={team.team_id}>
              <Pressable>
                <View className="flex-row items-center py-2 border-b border-border/40 last:border-b-0">
                  <TeamBadge team={team} size="sm" />
                  <Text className="ml-3 flex-1 text-text font-head">{team.display_name}</Text>
                  <Text className="text-muted font-sans text-xs">{team.city}</Text>
                </View>
              </Pressable>
            </Link>
          ))}
        </Card>
      )}
    </Screen>
  );
}
