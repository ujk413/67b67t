import { StyleSheet, Text, View } from "react-native";
import { telegramColors } from "../theme";

type DatePillProps = {
  text: string;
};

export function DatePill({ text }: DatePillProps) {
  return (
    <View style={styles.container}>
      <View style={styles.pill}>
        <Text style={styles.text}>{text}</Text>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    width: "100%",
    alignItems: "center",
    paddingVertical: 8,
  },
  pill: {
    backgroundColor: telegramColors.chat.serviceBackground,
    borderRadius: 12,
    paddingHorizontal: 12,
    paddingVertical: 6,
  },
  text: {
    fontSize: 13,
    fontWeight: "500",
    color: telegramColors.chat.serviceText,
  },
});
