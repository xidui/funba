import { Link } from "expo-router";
import { Pressable, Text, View, Linking } from "react-native";
import { Screen } from "../../components/Screen";
import { Card, SectionTitle } from "../../components/Card";
import { LoadingView, ErrorView, EmptyView } from "../../components/LoadError";
import { useMyMetrics } from "../../lib/queries";
import { useAppStore } from "../../lib/store";
import { API_BASE_URL } from "../../lib/api";
import { t } from "../../lib/i18n";

export default function MyMetricsScreen() {
  const user = useAppStore((s) => s.user);
  const { data, isLoading, isError, refetch, isFetching } = useMyMetrics();

  if (!user) {
    return (
      <Screen>
        <Text className="text-text font-head text-2xl mb-3">{t("my_metrics")}</Text>
        <Card>
          <Text className="text-muted font-sans">{t("need_login")}</Text>
          <Link href="/login" asChild>
            <Pressable className="bg-accent rounded-full px-4 py-2 mt-3 self-start">
              <Text className="text-black font-head">{t("login")}</Text>
            </Pressable>
          </Link>
        </Card>
      </Screen>
    );
  }

  if (isLoading) return <Screen><LoadingView /></Screen>;
  if (isError) return <Screen><ErrorView onRetry={refetch} /></Screen>;

  const webUrl = `${API_BASE_URL}/metrics/new`;
  return (
    <Screen refreshing={isFetching} onRefresh={refetch}>
      <Text className="text-text font-head text-2xl mb-3">{t("my_metrics")}</Text>

      <Pressable
        onPress={() => Linking.openURL(webUrl)}
        className="bg-accent rounded-full px-4 py-3 mb-4 items-center"
      >
        <Text className="text-black font-head">{t("create_on_web")} ↗</Text>
      </Pressable>

      <SectionTitle>{t("drafts")}</SectionTitle>
      {(data?.drafts ?? []).length === 0 ? <EmptyView /> : (
        <View>
          {data.drafts.map((m: any) => (
            <Link href={`/metrics/${m.key}`} asChild key={m.key}>
              <Pressable>
                <Card className="mb-2">
                  <Text className="text-text font-head">{m.name}</Text>
                  <Text className="text-muted font-sans text-[10px] uppercase mt-1">{m.status}</Text>
                </Card>
              </Pressable>
            </Link>
          ))}
        </View>
      )}

      <SectionTitle>{t("published")}</SectionTitle>
      {(data?.published ?? []).length === 0 ? <EmptyView /> : (
        <View>
          {data.published.map((m: any) => (
            <Link href={`/metrics/${m.key}`} asChild key={m.key}>
              <Pressable>
                <Card className="mb-2">
                  <Text className="text-text font-head">{m.name}</Text>
                </Card>
              </Pressable>
            </Link>
          ))}
        </View>
      )}
    </Screen>
  );
}
