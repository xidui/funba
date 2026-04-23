import { ActivityIndicator, Pressable, Text, View } from "react-native";
import { colors } from "../lib/theme";
import { t } from "../lib/i18n";

export function LoadingView() {
  return (
    <View className="py-20 items-center">
      <ActivityIndicator color={colors.accent} />
      <Text className="text-muted mt-3 font-sans">{t("loading")}</Text>
    </View>
  );
}

export function ErrorView({ onRetry }: { onRetry?: () => void }) {
  return (
    <View className="py-20 items-center">
      <Text className="text-loss font-head">{t("error_generic")}</Text>
      {onRetry ? (
        <Pressable onPress={onRetry} className="mt-4 px-4 py-2 bg-surface rounded-full border border-border">
          <Text className="text-text font-head">{t("retry")}</Text>
        </Pressable>
      ) : null}
    </View>
  );
}

export function EmptyView({ label }: { label?: string }) {
  return (
    <View className="py-20 items-center">
      <Text className="text-muted font-sans">{label ?? t("empty")}</Text>
    </View>
  );
}
