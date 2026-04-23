import { useState } from "react";
import { Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { Screen } from "../components/Screen";
import { Card, StatChip } from "../components/Card";
import { EmptyView, LoadingView } from "../components/LoadError";
import { usePlayerCompare, usePlayerHints } from "../lib/queries";
import { num } from "../lib/format";
import { colors } from "../lib/theme";
import { t } from "../lib/i18n";

export default function CompareScreen() {
  const [ids, setIds] = useState<{ id: string; name: string }[]>([]);
  const [q, setQ] = useState("");
  const hints = usePlayerHints(q);
  const compare = usePlayerCompare(ids.map((x) => x.id));

  return (
    <Screen>
      <Text className="text-text font-head text-2xl mb-1">{t("compare")}</Text>
      <Text className="text-muted font-sans text-xs mb-3">{t("compare_hint")}</Text>

      <View className="flex-row flex-wrap mb-3">
        {ids.map((x) => (
          <Pressable
            key={x.id}
            onPress={() => setIds((prev) => prev.filter((y) => y.id !== x.id))}
            className="bg-surface2 border border-border rounded-full px-3 py-1.5 mr-2 mb-2 flex-row items-center"
          >
            <Text className="text-text font-head text-xs">{x.name}</Text>
            <Text className="text-muted font-head text-xs ml-2">×</Text>
          </Pressable>
        ))}
      </View>

      {ids.length < 4 && (
        <>
          <TextInput
            value={q}
            onChangeText={setQ}
            placeholder={t("add_player")}
            placeholderTextColor={colors.muted}
            autoCapitalize="none"
            className="bg-surface border border-border rounded-2xl px-4 py-3 text-text font-sans mb-3"
          />
          {q.length >= 2 && (
            <ScrollView horizontal showsHorizontalScrollIndicator={false} className="mb-3">
              <View className="flex-row">
                {(hints.data?.players ?? []).map((p: any) => (
                  <Pressable
                    key={p.player_id}
                    onPress={() => {
                      if (!ids.find((x) => x.id === p.player_id)) {
                        setIds((prev) => [...prev, { id: p.player_id, name: p.display_name }]);
                      }
                      setQ("");
                    }}
                    className="bg-surface border border-border rounded-full px-3 py-1.5 mr-2"
                  >
                    <Text className="text-text font-head text-xs">{p.display_name}</Text>
                  </Pressable>
                ))}
              </View>
            </ScrollView>
          )}
        </>
      )}

      {ids.length < 2 ? (
        <EmptyView label={t("compare_hint")} />
      ) : compare.isLoading ? (
        <LoadingView />
      ) : (
        <View>
          {(compare.data?.players ?? []).map((p: any) => (
            <Card key={p.player_id} className="mb-3">
              <Text className="text-text font-head text-lg mb-2">{p.display_name}</Text>
              <ScrollView horizontal showsHorizontalScrollIndicator={false}>
                <View className="flex-row">
                  <StatChip label="GP" value={p.career.games ?? 0} />
                  <StatChip label="PPG" value={num(p.career.ppg)} />
                  <StatChip label="RPG" value={num(p.career.rpg)} />
                  <StatChip label="APG" value={num(p.career.apg)} />
                  <StatChip label="FG%" value={p.career.fg_pct != null ? `${(p.career.fg_pct * 100).toFixed(1)}` : "-"} />
                  <StatChip label="3P%" value={p.career.fg3_pct != null ? `${(p.career.fg3_pct * 100).toFixed(1)}` : "-"} />
                </View>
              </ScrollView>
            </Card>
          ))}
        </View>
      )}
    </Screen>
  );
}
