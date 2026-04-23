import { ReactNode } from "react";
import { ScrollView, View, RefreshControl } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { colors } from "../lib/theme";

type Props = {
  children: ReactNode;
  scroll?: boolean;
  refreshing?: boolean;
  onRefresh?: () => void;
  padded?: boolean;
};

export function Screen({ children, scroll = true, refreshing, onRefresh, padded = true }: Props) {
  const content = <View className={padded ? "px-4 py-4" : ""}>{children}</View>;
  return (
    <SafeAreaView edges={["left", "right"]} style={{ flex: 1, backgroundColor: colors.bg }}>
      {scroll ? (
        <ScrollView
          style={{ flex: 1 }}
          contentContainerStyle={{ paddingBottom: 48 }}
          refreshControl={onRefresh ? <RefreshControl tintColor={colors.accent} refreshing={Boolean(refreshing)} onRefresh={onRefresh} /> : undefined}
        >
          {content}
        </ScrollView>
      ) : (
        content
      )}
    </SafeAreaView>
  );
}
