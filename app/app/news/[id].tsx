import { Link, useLocalSearchParams } from "expo-router";
import { Pressable, Text, View, Image, Linking } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, SectionTitle } from "../../components/Card";
import { TeamBadge } from "../../components/TeamBadge";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useNewsDetail } from "../../lib/queries";
import { formatRelative } from "../../lib/format";
import { t } from "../../lib/i18n";

export default function NewsDetailScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const { data, isLoading, isError, refetch, isFetching } = useNewsDetail(id);

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError || !data?.article) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const a = data.article;
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      {a.thumbnail_url ? <Image source={{ uri: a.thumbnail_url }} className="w-full h-56 rounded-2xl mb-3" resizeMode="cover" /> : null}
      <Text className="text-text font-head text-xl mb-2">{a.title}</Text>
      <View className="flex-row mb-3">
        <Text className="text-accent font-head text-[10px] uppercase">{a.source}</Text>
        <Text className="text-muted font-sans text-[10px] ml-3">{formatRelative(a.published_at)}</Text>
      </View>
      {a.summary ? <Text className="text-text font-sans leading-5 mb-3">{a.summary}</Text> : null}
      <Pressable onPress={() => Linking.openURL(a.url)} className="bg-accent rounded-full px-4 py-3 mb-4 items-center">
        <Text className="text-black font-head">Read full article ↗</Text>
      </Pressable>

      {(data.players ?? []).length > 0 && (
        <>
          <SectionTitle>{t("players")}</SectionTitle>
          <Card>
            {data.players.map((p: any) => (
              <Link href={`/players/${p.slug}`} asChild key={p.player_id}>
                <Pressable className="flex-row items-center py-2 border-b border-border/40 last:border-b-0">
                  <Text className="text-text font-head flex-1">{p.display_name}</Text>
                </Pressable>
              </Link>
            ))}
          </Card>
        </>
      )}

      {(data.teams ?? []).length > 0 && (
        <>
          <SectionTitle>{t("teams")}</SectionTitle>
          <Card>
            {data.teams.map((team: any) => (
              <Link href={`/teams/${team.slug}`} asChild key={team.team_id}>
                <Pressable className="flex-row items-center py-2 border-b border-border/40 last:border-b-0">
                  <TeamBadge team={team} size="sm" />
                  <Text className="ml-3 flex-1 text-text font-head">{team.display_name}</Text>
                </Pressable>
              </Link>
            ))}
          </Card>
        </>
      )}

      {(data.siblings ?? []).length > 0 && (
        <>
          <SectionTitle>Related coverage</SectionTitle>
          <View>
            {data.siblings.map((s: any) => (
              <Pressable key={s.id} onPress={() => Linking.openURL(s.url)}>
                <Card className="mb-2">
                  <Text className="text-text font-head">{s.title}</Text>
                  <Text className="text-muted font-sans text-[10px] mt-1">{s.source} · {formatRelative(s.published_at)}</Text>
                </Card>
              </Pressable>
            ))}
          </View>
        </>
      )}
    </Screen>
  );
}
