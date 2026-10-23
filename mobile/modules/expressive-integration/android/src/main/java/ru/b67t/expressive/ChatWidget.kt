package ru.b67t.expressive

import android.app.PendingIntent
import android.appwidget.AppWidgetManager
import android.appwidget.AppWidgetProvider
import android.content.Context
import android.content.Intent
import android.widget.RemoteViews

/** Deliberately has no account, room, sender or message text on the lock screen. */
class ChatWidget : AppWidgetProvider() {
  override fun onUpdate(context: Context, manager: AppWidgetManager, ids: IntArray) {
    val launch = context.packageManager.getLaunchIntentForPackage(context.packageName) ?: return
    launch.addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP)
    launch.putExtra("expressive_action", "open_chats")
    val pending = PendingIntent.getActivity(context, 0, launch, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    for (id in ids) {
      val view = RemoteViews(context.packageName, R.layout.expressive_widget)
      val unread=context.getSharedPreferences("67chat-widget",0).getInt("unread",0)
      val english=context.getSharedPreferences("67chat-ui",0).getString("language","ru")=="en"
      view.setTextViewText(R.id.expressive_widget_count,if(english){if(unread>0)"Unread: $unread" else "No unread messages"}else{if(unread>0)"Непрочитанных: $unread" else "Нет непрочитанных"})
      view.setOnClickPendingIntent(R.id.expressive_widget_root, pending)
      manager.updateAppWidget(id, view)
    }
  }
}
