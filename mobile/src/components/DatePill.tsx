import { useTheme } from "../ThemeProvider";
import { StyleSheet, Text, View } from "react-native";
import { telegramColors } from "../theme";

type DatePillProps = {
  text: string;
};

export function DatePill({ text }: DatePillProps) {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  return (
    <View style={styles.container}>
      <View style={styles.pill}>
        <Text style={styles.text}>{text}</Text>
      </View>
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
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
};
