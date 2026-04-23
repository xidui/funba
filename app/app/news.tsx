import { Link } from "expo-router";
import { Pressable, Text, View, Image } from "react-native";
import { Screen } from "../components/Screen";
import { Card } from "../components/Card";
import { LoadingView, ErrorView, EmptyView } from "../components/LoadError";
import { useNewsList } from "../lib/queries";
import { formatRelative } from "../lib/format";
import { t } from "../lib/i18n";

export default function NewsScreen() {
  const { data, isLoading, isError, refetch, isFetching } = useNewsList();
  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError) return <Screen><ErrorView onRetry={refetch} /></Screen>;
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("news")}</Text>
      {(data?.news ?? []).length === 0 ? <EmptyView /> : (
        <View>
          {data.news.map((cluster: any) => {
            const a = cluster.article;
            if (!a) return null;
            return (
              <Link href={`/news/${cluster.cluster_id}`} asChild key={cluster.cluster_id}>
                <Pressable>
                  <Card className="mb-3">
                    {a.thumbnail_url ? (
                      <Image source={{ uri: a.thumbnail_url }} className="w-full h-44 rounded-xl mb-2" resizeMode="cover" />
                    ) : null}
                    <Text className="text-text font-head text-base">{a.title}</Text>
                    {a.summary ? (
                      <Text className="text-muted font-sans text-xs mt-1" numberOfLines={2}>{a.summary}</Text>
                    ) : null}
                    <View className="flex-row mt-2 items-center">
                      <Text className="text-accent font-head text-[10px] uppercase">{a.source}</Text>
                      <Text className="text-muted font-sans text-[10px] ml-3">{formatRelative(a.published_at)}</Text>
                      {cluster.article_count > 1 && (
                        <Text className="text-muted font-sans text-[10px] ml-3">· {cluster.article_count} sources</Text>
                      )}
                    </View>
                  </Card>
                </Pressable>
              </Link>
            );
          })}
        </View>
      )}
    </Screen>
  );
}
