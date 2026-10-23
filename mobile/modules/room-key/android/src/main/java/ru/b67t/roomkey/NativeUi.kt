package ru.b67t.roomkey

import android.content.Context

internal object NativeUi {
  fun text(context:Context,ru:String,en:String):String = if(context.getSharedPreferences("67chat-ui",Context.MODE_PRIVATE).getString("language","ru")=="en")en else ru
  fun setLanguage(context:Context,language:String){context.getSharedPreferences("67chat-ui",Context.MODE_PRIVATE).edit().putString("language",if(language=="en")"en" else "ru").apply()}
}
