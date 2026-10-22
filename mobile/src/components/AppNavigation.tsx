import { useEffect, type ReactNode } from "react";
import {
  NavigationContainer,
  createNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
const Stack = createNativeStackNavigator();
const navigation = createNavigationContainerRef<{
  Chats: undefined;
  Chat: undefined;
  Workspace: undefined;
  Profile: undefined;
}>();
export function AppNavigation({
  roomId,
  list,
  chat,
  onBack,
  workspace,
  profile,
  onCloseWorkspace,
  onCloseProfile,
}: {
  roomId: string | null;
  list: () => ReactNode;
  chat: () => ReactNode;
  onBack: () => void;
  workspace?: () => ReactNode;
  profile?: () => ReactNode;
  onCloseWorkspace: () => void;
  onCloseProfile: () => void;
}) {
  const align = () => {
    if (!navigation.isReady()) return;
    const current = navigation.getRootState()?.routes || [];
    const names = [
      "Chats",
      ...(roomId ? ["Chat"] : []),
      ...(workspace ? ["Workspace"] : []),
      ...(profile ? ["Profile"] : []),
    ];
    if (current.map((route) => route.name).join() === names.join()) return;
    navigation.resetRoot({
      index: names.length - 1,
      routes: names.map((name) => ({
        name,
        key: current.find((route) => route.name === name)?.key,
      })),
    });
  };
  useEffect(align, [roomId, !!workspace, !!profile]);
  return (
    <NavigationContainer
      ref={navigation}
      onReady={align}
      onStateChange={() => {
        const names = (navigation.getRootState()?.routes || []).map(
          (route) => route.name,
        );
        if (workspace && !names.includes("Workspace")) onCloseWorkspace();
        if (profile && !names.includes("Profile")) onCloseProfile();
        if (roomId && !names.includes("Chat")) onBack();
      }}
    >
      <Stack.Navigator
        screenOptions={{ headerShown: false, animation: "slide_from_right" }}
      >
        <Stack.Screen name="Chats">{() => list()}</Stack.Screen>
        <Stack.Screen name="Chat">{() => chat()}</Stack.Screen>
        <Stack.Screen name="Workspace">{() => workspace?.()}</Stack.Screen>
        <Stack.Screen name="Profile">{() => profile?.()}</Stack.Screen>
      </Stack.Navigator>
    </NavigationContainer>
  );
}
