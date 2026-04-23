import { Tabs } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { colors } from "../../lib/theme";
import { t } from "../../lib/i18n";
import { useAppStore } from "../../lib/store";

export default function TabsLayout() {
  // subscribe so tab labels re-render on language change
  useAppStore((s) => s.lang);
  return (
    <Tabs
      screenOptions={{
        headerStyle: { backgroundColor: colors.bg },
        headerTintColor: colors.text,
        headerTitleStyle: { color: colors.text, fontFamily: "Outfit" },
        tabBarStyle: {
          backgroundColor: colors.bg,
          borderTopColor: colors.border,
        },
        tabBarActiveTintColor: colors.accent,
        tabBarInactiveTintColor: colors.muted,
        sceneStyle: { backgroundColor: colors.bg },
      }}
    >
      <Tabs.Screen
        name="index"
        options={{
          title: t("home"),
          tabBarIcon: ({ color, size }) => <Ionicons name="home-outline" color={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="games"
        options={{
          title: t("games"),
          tabBarIcon: ({ color, size }) => <Ionicons name="basketball-outline" color={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="metrics"
        options={{
          title: t("metrics"),
          tabBarIcon: ({ color, size }) => <Ionicons name="stats-chart-outline" color={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="account"
        options={{
          title: t("account"),
          tabBarIcon: ({ color, size }) => <Ionicons name="person-outline" color={color} size={size} />,
        }}
      />
    </Tabs>
  );
}
