import {t as uiText} from "../../../web/i18n.js";
import { useTheme } from "../ThemeProvider";
import { useState } from "react";
import {TransportSettings} from '../features/TransportSettings';
import {
  ActivityIndicator,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { telegramColors, telegramMetrics } from "../theme";

type AuthScreenProps = {
  authMode: "login" | "register";
  username: string;
  password: string;
  error: string | null;
  busy: boolean;
  onChangeMode: (mode: "login" | "register") => void;
  onChangeUsername: (text: string) => void;
  onChangePassword: (text: string) => void;
  onSubmit: () => void;
  onQR?:()=>void;
};

export function AuthScreen({
  authMode,
  username,
  password,
  error,
  busy,
  onChangeMode,
  onChangeUsername,
  onChangePassword,
  onSubmit,
  onQR,
}: AuthScreenProps) {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();
  const [network,setNetwork]=useState(false);
  if(network)return <TransportSettings onClose={()=>setNetwork(false)}/>;

  return (
    <View style={styles.container}>
      <View style={styles.card}>
        <Text style={styles.brand}>67</Text>
        <Text style={styles.subtitle}>{uiText("Личные сообщения и звонки")}</Text>

        <View style={styles.modeRow}>
          <Pressable
            style={[
              styles.modeButton,
              authMode === "login" && styles.modeButtonActive,
            ]}
            disabled={busy}
            onPress={() => onChangeMode("login")}
          >
            <Text
              style={[
                styles.modeButtonText,
                authMode === "login" && styles.modeButtonTextActive,
              ]}
            >
              {uiText("Вход")}</Text>
          </Pressable>
          <Pressable
            style={[
              styles.modeButton,
              authMode === "register" && styles.modeButtonActive,
            ]}
            disabled={busy}
            onPress={() => onChangeMode("register")}
          >
            <Text
              style={[
                styles.modeButtonText,
                authMode === "register" && styles.modeButtonTextActive,
              ]}
            >
              {uiText("Регистрация")}</Text>
          </Pressable>
        </View>

        <TextInput
          style={styles.input}
          value={username}
          onChangeText={onChangeUsername}
          placeholder={uiText("Имя пользователя")}
          placeholderTextColor={telegramColors.composer.hint}
          autoCapitalize="none"
          autoCorrect={false}
          editable={!busy}
        />

        <TextInput
          style={styles.input}
          value={password}
          onChangeText={onChangePassword}
          placeholder={uiText("Пароль")}
          placeholderTextColor={telegramColors.composer.hint}
          secureTextEntry
          editable={!busy}
        />

        {error && <Text style={styles.error}>{error}</Text>}

        <Pressable
          style={[styles.button, busy && styles.buttonDisabled]}
          onPress={onSubmit}
          disabled={busy}
        >
          {busy ? (
            <ActivityIndicator
              size="small"
              color={telegramColors.text.onAccent}
            />
          ) : (
            <Text style={styles.buttonText}>
              {authMode === "login" ? uiText("Войти") : uiText("Создать аккаунт")}
            </Text>
          )}
        </Pressable>
        {authMode==='login'&&onQR&&<Pressable accessibilityRole="button" onPress={onQR} disabled={busy} style={{padding:14,minHeight:48}}><Text style={{color:telegramColors.text.blue,textAlign:'center'}}>{uiText("Войти по QR-коду")}</Text></Pressable>}
        <Pressable accessibilityRole="button" onPress={()=>setNetwork(true)} disabled={busy} style={{padding:14,minHeight:48}}><Text style={{color:telegramColors.text.blue,textAlign:'center'}}>{uiText("Сеть, прокси и Tor")}</Text></Pressable>
      </View>
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    container: {
      flex: 1,
      backgroundColor: telegramColors.windowBackground,
      alignItems: "center",
      justifyContent: "center",
      padding: 20,
    },
    card: {
      width: "100%",
      maxWidth: 400,
      backgroundColor: telegramColors.composer.background,
      borderRadius: 16,
      padding: 24,
      gap: 16,
    },
    brand: {
      fontSize: 48,
      fontWeight: "800",
      color: telegramColors.text.primary,
      textAlign: "center",
    },
    subtitle: {
      fontSize: 16,
      color: telegramColors.text.gray5,
      textAlign: "center",
      marginBottom: 8,
    },
    modeRow: {
      flexDirection: "row",
      gap: 8,
    },
    modeButton: {
      flex: 1,
      paddingVertical: 12,
      borderRadius: 8,
      backgroundColor: telegramColors.windowBackgroundGray,
      alignItems: "center",
    },
    modeButtonActive: {
      backgroundColor: telegramColors.text.blue,
    },
    modeButtonText: {
      fontSize: 15,
      fontWeight: "600",
      color: telegramColors.text.secondary,
    },
    modeButtonTextActive: {
      color: telegramColors.text.onAccent,
    },
    input: {
      backgroundColor: telegramColors.windowBackgroundGray,
      borderRadius: 8,
      paddingHorizontal: 16,
      paddingVertical: 14,
      fontSize: telegramMetrics.font.messageText,
      color: telegramColors.text.primary,
    },
    error: {
      fontSize: 14,
      color: telegramColors.misc.error,
      textAlign: "center",
    },
    button: {
      backgroundColor: telegramColors.text.blue,
      borderRadius: 8,
      paddingVertical: 14,
      alignItems: "center",
      justifyContent: "center",
      minHeight: 48,
    },
    buttonDisabled: {
      opacity: 0.6,
    },
    buttonText: {
      fontSize: 16,
      fontWeight: "700",
      color: telegramColors.text.onAccent,
    },
  });
};
