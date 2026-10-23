package ru.b67t.roomkey

import android.content.Context
import android.media.MediaMetadataRetriever
import android.net.Uri
import android.os.Handler
import android.os.Looper
import androidx.media3.common.MediaItem
import androidx.media3.common.MimeTypes
import androidx.media3.effect.Presentation
import androidx.media3.transformer.Composition
import androidx.media3.transformer.EditedMediaItem
import androidx.media3.transformer.Effects
import androidx.media3.transformer.ExportException
import androidx.media3.transformer.ExportResult
import androidx.media3.transformer.ProgressHolder
import androidx.media3.transformer.Transformer
import java.io.File
import java.net.URI

@androidx.media3.common.util.UnstableApi
class MediaTranscoder(private val context: Context) {
  private val handler=Handler(Looper.getMainLooper())
  private val jobs=mutableMapOf<String,Pair<Transformer,()->Unit>>()
  // Cancellation may arrive while metadata is still being read on the worker.
  private val cancelled=java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
  private fun describe(file:File, uri:String=file.toURI().toString()):Map<String,Any> {
    val metadata=MediaMetadataRetriever()
    try {
      metadata.setDataSource(file.path)
      val width=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_WIDTH)?.toIntOrNull() ?: 0
      val height=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_HEIGHT)?.toIntOrNull() ?: 0
      val rotation=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_ROTATION)?.toIntOrNull() ?: 0
      val duration=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DURATION)?.toLongOrNull() ?: 0L
      require(width>0 && height>0)
      return mapOf("uri" to uri,"width" to if(rotation%180==0)width else height,"height" to if(rotation%180==0)height else width,"rotation" to rotation,"orientation" to 1,"duration_ms" to duration)
    } finally {metadata.release()}
  }
  fun cancel(id:String) {cancelled.add(id);handler.post { jobs.remove(id)?.let {it.first.cancel();it.second();cancelled.remove(id)} }}
  fun start(inputUri:String,height:Int,id:String,progress:(Double)->Unit,done:(Map<String,Any>)->Unit,fail:(Throwable)->Unit) {
    val input=File(URI(inputUri)).canonicalFile
    require(input.path.startsWith(context.filesDir.canonicalPath+File.separator) || input.path.startsWith(context.cacheDir.canonicalPath+File.separator))
    require(height==480 || height==720)
    val metadata=MediaMetadataRetriever()
    val width:Int;val sourceHeight:Int;val rotation:Int
    try {metadata.setDataSource(input.path);width=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_WIDTH)?.toIntOrNull() ?: 0;sourceHeight=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_HEIGHT)?.toIntOrNull() ?: 0;rotation=metadata.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_ROTATION)?.toIntOrNull() ?: 0;} finally {metadata.release()}
    require(width>0 && sourceHeight>0)
    val displayWidth=if(rotation%180==0)width else sourceHeight
    val displayHeight=if(rotation%180==0)sourceHeight else width
    if(minOf(displayWidth,displayHeight)<=height){if(cancelled.remove(id))fail(IllegalStateException("processing_cancelled")) else done(describe(input,inputUri));return}
    val output=File(context.cacheDir,"processed-video-${java.util.UUID.randomUUID()}.mp4")
    val outputHeight=maxOf(2,((displayHeight.toDouble()*height/minOf(displayWidth,displayHeight)).toInt()/2)*2)
    handler.post {
      if(cancelled.remove(id)){fail(IllegalStateException("processing_cancelled"));return@post}
      if(jobs.containsKey(id)){fail(IllegalStateException("processing_busy"));return@post}
      var finished=false
      fun failure(error:Throwable){if(finished)return;finished=true;jobs.remove(id);output.delete();fail(error)}
      val transformer=Transformer.Builder(context).setVideoMimeType(MimeTypes.VIDEO_H264).setAudioMimeType(MimeTypes.AUDIO_AAC)
        .addListener(object:Transformer.Listener {
          override fun onCompleted(composition:Composition,result:ExportResult) {
            if(finished)return
            try {
              val actual=describe(output)
              if(cancelled.remove(id)){failure(IllegalStateException("processing_cancelled"));return}
              finished=true;jobs.remove(id);done(actual)
            } catch(error:Throwable){failure(error)}
          }
          override fun onError(composition:Composition,result:ExportResult,exception:ExportException){failure(exception)}
        }).build()
      jobs[id]=Pair(transformer,{failure(IllegalStateException("processing_cancelled"))})
      val item=EditedMediaItem.Builder(MediaItem.fromUri(Uri.fromFile(input))).setEffects(Effects(emptyList(),listOf(Presentation.createForHeight(outputHeight)))).build()
      try {transformer.start(item,output.path)}catch(error:Throwable){failure(error);return@post}
      val holder=ProgressHolder()
      val poll=object:Runnable {override fun run(){if(finished)return;try{if(transformer.getProgress(holder)==Transformer.PROGRESS_STATE_AVAILABLE)progress(holder.progress/100.0)}catch(_:Throwable){};handler.postDelayed(this,250)}}
      handler.post(poll)
    }
  }
}
