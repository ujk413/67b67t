import { useTheme } from "../ThemeProvider";
import { Icon } from "./Icon";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { telegramColors } from "../theme";

type MainHeaderProps = {
  title: string;
  onSearchPress: () => void;
  onMenuPress: () => void;
};

export function MainHeader({
  title,
  onSearchPress,
  onMenuPress,
}: MainHeaderProps) {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  return (
    <View style={styles.container}>
      <Text style={styles.title}>{title}</Text>

      <View style={styles.actions}>
        <Pressable
          onPress={onSearchPress}
          hitSlop={8}
          style={styles.iconButton}
          accessibilityRole="button"
          accessibilityLabel="Поиск"
        >
          <Icon name="search" />
        </Pressable>
        <Pressable
          onPress={onMenuPress}
          hitSlop={8}
          style={styles.iconButton}
          accessibilityRole="button"
          accessibilityLabel="Меню"
        >
          <Icon name="more" />
        </Pressable>
      </View>
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    container: {
      height: 56,
      backgroundColor: telegramColors.actionBar,
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "space-between",
      paddingHorizontal: 16,
    },
    title: {
      fontSize: 22,
      fontWeight: "700",
      color: telegramColors.actionBarTitle,
    },
    actions: {
      flexDirection: "row",
      gap: 0,
    },
    iconButton: {
      width: 48,
      height: 48,
      alignItems: "center",
      justifyContent: "center",
    },
    icon: {
      fontSize: 22,
      color: telegramColors.actionBarIcon,
    },
    menuIcon: {
      fontSize: 28,
      fontWeight: "700",
      color: telegramColors.actionBarIcon,
    },
  });
};
