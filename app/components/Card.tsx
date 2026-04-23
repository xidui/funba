import { ReactNode } from "react";
import { View, Text, Pressable } from "react-native";

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <View className={`bg-surface rounded-2xl p-4 border border-border ${className}`}>{children}</View>
  );
}

export function SectionTitle({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <View className="flex-row items-baseline justify-between mb-2 mt-6">
      <Text className="text-text font-head text-lg">{children}</Text>
      {right}
    </View>
  );
}

export function StatChip({ label, value }: { label: string; value: string | number }) {
  return (
    <View className="bg-surface2 rounded-xl px-3 py-2 mr-2 mb-2">
      <Text className="text-muted text-[10px] font-head uppercase tracking-wide">{label}</Text>
      <Text className="text-text font-head text-lg">{value}</Text>
    </View>
  );
}

export function PillButton({ label, active = false, onPress }: { label: string; active?: boolean; onPress?: () => void }) {
  return (
    <Pressable
      onPress={onPress}
      className={`px-3 py-1.5 rounded-full mr-2 mb-2 border ${active ? "bg-accent border-accent" : "bg-surface border-border"}`}
    >
      <Text className={active ? "text-black font-head text-xs" : "text-text font-head text-xs"}>{label}</Text>
    </Pressable>
  );
}
