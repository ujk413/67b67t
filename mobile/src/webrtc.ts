/**
 * WebRTC for voice and video calls
 * React Native implementation
 */
import {assertCallsAllowed} from './accountRegistry';

import {
  RTCPeerConnection,
  RTCIceCandidate,
  RTCSessionDescription,
  mediaDevices,
  MediaStream,
  MediaStreamTrack,
} from '@livekit/react-native-webrtc';

export interface CallConfig {
  audio: boolean;
  video: boolean;
}

export interface CallEvents {
  onLocalStream?: (stream: MediaStream) => void;
  onRemoteStream?: (stream: MediaStream) => void;
  onIceCandidate?: (candidate: RTCIceCandidate) => void;
  onConnectionStateChange?: (state: string) => void;
  onError?: (error: Error) => void;
  onRenegotiation?: (offer: RTCSessionDescriptionInit) => void;
  onScreenShareChange?: (enabled: boolean) => void;
}

export class WebRTCCall {
  private pc: RTCPeerConnection | null = null;
  private localStream: MediaStream | null = null;
  private remoteStream: MediaStream | null = null;
  private config: CallConfig;
  private events: CallEvents;
  private isInitiator: boolean;
  private iceServers: any[] | undefined;
  private pendingCandidates: RTCIceCandidateInit[] = [];
  private remoteReady = false;
  private screenStream: MediaStream | null = null;
  private restarting = false;
  private restartAttempts = 0;
  private restartTimer: ReturnType<typeof setTimeout> | null = null;
  private generation = 0;

  constructor(config: CallConfig, events: CallEvents, isInitiator: boolean, iceServers?: any[]) {
    this.config = config;
    this.events = events;
    this.isInitiator = isInitiator;
    this.iceServers = iceServers;
  }

  async start(): Promise<void> {
    await assertCallsAllowed();
    const generation=++this.generation;
    try {
      // Get user media
      const constraints = {
        // The native declaration omits W3C audio processing flags; retain a supported
        // field so structural typing accepts this object without an unsafe cast.
        audio: this.config.audio ? {deviceId: 'default', echoCancellation: true, noiseSuppression: true, autoGainControl: true} : false,
        video: this.config.video ? {
          width: { ideal: 1280 },
          height: { ideal: 720 },
          frameRate: { ideal: 30 },
          facingMode: 'user',
        } : false,
      };

      const captured = await mediaDevices.getUserMedia(constraints);
      if(generation!==this.generation){captured.getTracks().forEach(track=>track.stop());return;}
      this.localStream = captured;
      this.events.onLocalStream?.(this.localStream);

      // Create peer connection
      this.pc = new RTCPeerConnection({
        // Server-issued, time-limited TURN credentials (GET /rtc/ice-servers).
        iceServers: this.iceServers && this.iceServers.length
          ? this.iceServers
          : [],
      });

      // Add local tracks
      this.localStream.getTracks().forEach(track => {
        if (this.pc && this.localStream) {
          this.pc.addTrack(track, this.localStream);
        }
      });

      // Handle remote stream
      (this.pc as any).ontrack = (event: any) => {
        if (event.streams && event.streams[0]) {
          this.remoteStream = event.streams[0];
          this.events.onRemoteStream?.(this.remoteStream as MediaStream);
        }
      };

      // Handle ICE candidates
      (this.pc as any).onicecandidate = (event: any) => {
        if (event.candidate) {
          this.events.onIceCandidate?.(event.candidate);
        }
      };

      // Handle connection state
      (this.pc as any).onconnectionstatechange = () => {
        if (this.pc) {
          const state=this.pc.connectionState || 'unknown';
          this.events.onConnectionStateChange?.(state);
          if(state==='connected'){this.restartAttempts=0;if(this.restartTimer)clearTimeout(this.restartTimer);this.restartTimer=null;}
          if(state==='failed'||state==='disconnected'){
            if(this.restartTimer)clearTimeout(this.restartTimer);
            this.restartTimer=setTimeout(()=>{this.restartTimer=null;void this.restartIce().catch(error=>this.events.onError?.(error));},state==='failed'?0:2500);
          }
        }
      };

      // Create offer if initiator
      if (this.isInitiator) {
        const offer = await this.pc.createOffer({
          offerToReceiveAudio: true,
          offerToReceiveVideo: this.config.video,
        });
        await this.pc.setLocalDescription(offer);
      }
    } catch (error) {
      await this.end();
      this.events.onError?.(error as Error);
      throw error;
    }
  }

  /** Call after a network-type change. Only the initial caller negotiates to avoid glare. */
  async restartIce(): Promise<void> {
    if(!this.pc||!this.isInitiator||this.restarting||this.pc.signalingState!=='stable')return;
    if(++this.restartAttempts>3){await this.end();this.events.onError?.(new Error('Не удалось восстановить звонок'));return;}
    this.restarting=true;const pc=this.pc,generation=this.generation;
    try {const offer=await pc.createOffer({iceRestart:true});if(generation!==this.generation)return;await pc.setLocalDescription(offer);this.events.onRenegotiation?.(offer);}
    finally{this.restarting=false;}
  }

  async startScreenShare(): Promise<void> {
    await assertCallsAllowed();
    if(!this.pc||this.screenStream)return;
    const generation=this.generation;
    const stream=await mediaDevices.getDisplayMedia();
    if(generation!==this.generation){stream.getTracks().forEach(track=>track.stop());return;}
    const track=stream.getVideoTracks()[0],sender=this.pc.getSenders().find(item=>item.track?.kind==='video');
    // Audio-only calls need a negotiated video transceiver first; never pretend it was shared.
    if(!track||!sender){stream.getTracks().forEach(item=>item.stop());throw new Error('Демонстрация экрана доступна в видеозвонке');}
    try {await sender.replaceTrack(track);this.screenStream=stream;(track as any).onended=()=>{void this.stopScreenShare().catch(error=>this.events.onError?.(error));};this.events.onScreenShareChange?.(true);}
    catch(error){stream.getTracks().forEach(item=>item.stop());throw error;}
  }

  async stopScreenShare(): Promise<void> {
    const stream=this.screenStream;if(!stream)return;this.screenStream=null;
    try{const sender=this.pc?.getSenders().find(item=>item.track?.kind==='video');if(sender)await sender.replaceTrack(this.localStream?.getVideoTracks()[0]||null);}
    finally{stream.getTracks().forEach(track=>{(track as any).onended=null;track.stop();});this.events.onScreenShareChange?.(false);}
  }

  async handleOffer(offer: RTCSessionDescriptionInit): Promise<RTCSessionDescriptionInit> {
    if (!this.pc) throw new Error('Peer connection not initialized');

    await this.pc.setRemoteDescription(new RTCSessionDescription(offer as any));
    this.remoteReady = true;
    await this.flushCandidates();
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);
    return answer;
  }

  async handleAnswer(answer: RTCSessionDescriptionInit): Promise<void> {
    if (!this.pc) throw new Error('Peer connection not initialized');
    await this.pc.setRemoteDescription(new RTCSessionDescription(answer as any));
    this.remoteReady = true;
    await this.flushCandidates();
  }

  async addIceCandidate(candidate: RTCIceCandidateInit): Promise<void> {
    if (!this.pc || !this.remoteReady) { this.pendingCandidates.push(candidate); return; }
    await this.pc.addIceCandidate(new RTCIceCandidate(candidate));
  }

  private async flushCandidates() {
    if (!this.pc || !this.remoteReady) return;
    for (const candidate of this.pendingCandidates.splice(0)) await this.pc.addIceCandidate(new RTCIceCandidate(candidate));
  }

  getLocalDescription(): RTCSessionDescriptionInit | null {
    return this.pc?.localDescription as RTCSessionDescriptionInit | null;
  }

  toggleAudio(enabled: boolean): void {
    if (this.localStream) {
      this.localStream.getAudioTracks().forEach(track => {
        track.enabled = enabled;
      });
    }
  }

  toggleVideo(enabled: boolean): void {
    if (this.localStream) {
      this.localStream.getVideoTracks().forEach(track => {
        track.enabled = enabled;
      });
    }
  }

  async switchCamera(): Promise<void> {
    if (!this.localStream || !this.config.video) return;

    const videoTrack = this.localStream.getVideoTracks()[0];
    if (!videoTrack) return;

    // @ts-ignore - _switchCamera is available on React Native
    await videoTrack._switchCamera();
  }

  async end(): Promise<void> {
    ++this.generation;
    if(this.restartTimer)clearTimeout(this.restartTimer);this.restartTimer=null;
    this.screenStream?.getTracks().forEach(track=>{(track as any).onended=null;track.stop();});this.screenStream=null;
    this.pendingCandidates = [];
    this.remoteReady = false;
    if (this.localStream) {
      this.localStream.getTracks().forEach(track => track.stop());
      this.localStream = null;
    }

    if (this.remoteStream) {
      this.remoteStream.getTracks().forEach(track => track.stop());
      this.remoteStream = null;
    }

    if (this.pc) {
      (this.pc as any).onconnectionstatechange=null;(this.pc as any).onicecandidate=null;(this.pc as any).ontrack=null;
      this.pc.close();
      this.pc = null;
    }
  }

  getConnectionState(): string {
    return this.pc?.connectionState || 'closed';
  }
}
