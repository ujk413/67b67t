/**
 * WebRTC for voice and video calls
 * React Native implementation
 */

import {
  RTCPeerConnection,
  RTCIceCandidate,
  RTCSessionDescription,
  mediaDevices,
  MediaStream,
  MediaStreamTrack,
} from 'react-native-webrtc';

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

  constructor(config: CallConfig, events: CallEvents, isInitiator: boolean, iceServers?: any[]) {
    this.config = config;
    this.events = events;
    this.isInitiator = isInitiator;
    this.iceServers = iceServers;
  }

  async start(): Promise<void> {
    try {
      // Get user media
      const constraints = {
        audio: this.config.audio,
        video: this.config.video ? {
          width: { ideal: 1280 },
          height: { ideal: 720 },
          frameRate: { ideal: 30 },
          facingMode: 'user',
        } : false,
      };

      this.localStream = await mediaDevices.getUserMedia(constraints);
      this.events.onLocalStream?.(this.localStream);

      // Create peer connection
      this.pc = new RTCPeerConnection({
        // Server-issued, time-limited TURN credentials (GET /rtc/ice-servers).
        iceServers: this.iceServers && this.iceServers.length
          ? this.iceServers
          : [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }],
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
          this.events.onConnectionStateChange?.((this.pc as any).connectionState || "unknown");
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
      this.events.onError?.(error as Error);
      throw error;
    }
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
      this.pc.close();
      this.pc = null;
    }
  }

  getConnectionState(): string {
    return this.pc?.connectionState || 'closed';
  }
}
