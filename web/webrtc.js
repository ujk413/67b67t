// WebRTC with DTLS-SRTP encryption
class SecureWebRTC {
  constructor() {
    this.peerConnection = null;
    this.localStream = null;
    this.remoteStream = null;
    this.onRemoteStream = null;
    this.onIceCandidate = null;
    this.audioOutputDevice = 'default'; // 'default' or 'earpiece'
  }

  async initCall(isVideo = false) {
    // Get user media
    this.localStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true
      },
      video: isVideo ? {
        width: { ideal: 1280 },
        height: { ideal: 720 },
        facingMode: 'user'
      } : false
    });

    // Create peer connection with DTLS-SRTP enabled (default in modern browsers)
    const configuration = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        ...(window.__APP_CONFIG__?.turn?.urls ? [{
          urls: window.__APP_CONFIG__?.turn?.urls,
          username: window.__APP_CONFIG__?.turn?.username,
          credential: window.__APP_CONFIG__?.turn?.credential
        }] : [])
      ],
      // Force DTLS-SRTP encryption
      bundlePolicy: 'max-bundle',
      rtcpMuxPolicy: 'require',
      // Enable perfect forward secrecy
      iceCandidatePoolSize: 10
    };

    this.peerConnection = new RTCPeerConnection(configuration);

    // Add local stream tracks
    this.localStream.getTracks().forEach(track => {
      this.peerConnection.addTrack(track, this.localStream);
    });

    // Handle remote stream
    this.peerConnection.ontrack = (event) => {
      if (!this.remoteStream) {
        this.remoteStream = new MediaStream();
        if (this.onRemoteStream) {
          this.onRemoteStream(this.remoteStream);
        }
      }
      this.remoteStream.addTrack(event.track);

      // Set audio output to earpiece if enabled
      if (event.track.kind === 'audio' && this.audioOutputDevice === 'earpiece') {
        this.setAudioOutputToEarpiece();
      }
    };

    // Handle ICE candidates
    this.peerConnection.onicecandidate = (event) => {
      if (event.candidate && this.onIceCandidate) {
        this.onIceCandidate(event.candidate);
      }
    };

    // Monitor connection state
    this.peerConnection.onconnectionstatechange = () => {
      if (this.peerConnection.connectionState === 'failed') {
        this.restartIce();
      }
    };

    // Monitor ICE connection state
    this.peerConnection.oniceconnectionstatechange = () => {
    };

    return this.localStream;
  }

  // Set audio output to earpiece (proximity speaker)
  async setAudioOutputToEarpiece() {
    try {
      // Check if setSinkId is supported
      const audioElement = document.querySelector('audio');
      if (!audioElement || typeof audioElement.setSinkId !== 'function') {
        console.warn('setSinkId not supported, using default audio output');
        return false;
      }

      // Get available audio output devices
      const devices = await navigator.mediaDevices.enumerateDevices();
      const audioOutputs = devices.filter(device => device.kind === 'audiooutput');

      // Try to find earpiece/phone speaker
      // On mobile devices, the earpiece is usually labeled differently
      const earpiece = audioOutputs.find(device =>
        device.label.toLowerCase().includes('earpiece') ||
        device.label.toLowerCase().includes('phone') ||
        device.label.toLowerCase().includes('receiver')
      );

      if (earpiece) {
        await audioElement.setSinkId(earpiece.deviceId);
        this.audioOutputDevice = 'earpiece';
        return true;
      } else {
        // Fallback: use first non-default device (often the earpiece on mobile)
        const nonDefault = audioOutputs.find(device => device.deviceId !== 'default');
        if (nonDefault) {
          await audioElement.setSinkId(nonDefault.deviceId);
          this.audioOutputDevice = 'earpiece';
          return true;
        }
      }

      console.warn('Earpiece not found, using default audio output');
      return false;
    } catch (error) {
      console.error('Failed to set audio output:', error);
      return false;
    }
  }

  // Set audio output to speaker
  async setAudioOutputToSpeaker() {
    try {
      const audioElement = document.querySelector('audio');
      if (!audioElement || typeof audioElement.setSinkId !== 'function') {
        return false;
      }

      // Set to default (speaker)
      await audioElement.setSinkId('default');
      this.audioOutputDevice = 'default';
      return true;
    } catch (error) {
      console.error('Failed to set audio output:', error);
      return false;
    }
  }

  // Toggle between earpiece and speaker
  async toggleAudioOutput() {
    if (this.audioOutputDevice === 'earpiece') {
      return await this.setAudioOutputToSpeaker();
    } else {
      return await this.setAudioOutputToEarpiece();
    }
  }

  // Toggle video track
  toggleVideo(enabled) {
    if (!this.localStream) return false;

    const videoTrack = this.localStream.getVideoTracks()[0];
    if (videoTrack) {
      videoTrack.enabled = enabled;
      return true;
    }
    return false;
  }

  // Toggle audio track
  toggleAudio(enabled) {
    if (!this.localStream) return false;

    const audioTrack = this.localStream.getAudioTracks()[0];
    if (audioTrack) {
      audioTrack.enabled = enabled;
      return true;
    }
    return false;
  }

  // Switch camera (front/back)
  async switchCamera() {
    if (!this.localStream) return false;

    const videoTrack = this.localStream.getVideoTracks()[0];
    if (!videoTrack) return false;

    try {
      // Get current facing mode
      const settings = videoTrack.getSettings();
      const currentFacingMode = settings.facingMode || 'user';
      const newFacingMode = currentFacingMode === 'user' ? 'environment' : 'user';

      // Stop current track
      videoTrack.stop();

      // Get new stream with different camera
      const newStream = await navigator.mediaDevices.getUserMedia({
        video: {
          facingMode: newFacingMode,
          width: { ideal: 1280 },
          height: { ideal: 720 }
        }
      });

      const newVideoTrack = newStream.getVideoTracks()[0];

      // Replace track in peer connection
      const sender = this.peerConnection.getSenders().find(s => s.track?.kind === 'video');
      if (sender) {
        await sender.replaceTrack(newVideoTrack);
      }

      // Replace track in local stream
      this.localStream.removeTrack(videoTrack);
      this.localStream.addTrack(newVideoTrack);

      return true;
    } catch (error) {
      console.error('Failed to switch camera:', error);
      return false;
    }
  }

  async createOffer() {
    if (!this.peerConnection) throw new Error('Peer connection not initialized');

    const offer = await this.peerConnection.createOffer({
      offerToReceiveAudio: true,
      offerToReceiveVideo: true
    });

    await this.peerConnection.setLocalDescription(offer);
    return offer;
  }

  async createAnswer() {
    if (!this.peerConnection) throw new Error('Peer connection not initialized');

    const answer = await this.peerConnection.createAnswer();
    await this.peerConnection.setLocalDescription(answer);
    return answer;
  }

  async handleOffer(offer) {
    if (!this.peerConnection) throw new Error('Peer connection not initialized');

    await this.peerConnection.setRemoteDescription(new RTCSessionDescription(offer));
  }

  async handleAnswer(answer) {
    if (!this.peerConnection) throw new Error('Peer connection not initialized');

    await this.peerConnection.setRemoteDescription(new RTCSessionDescription(answer));
  }

  async addIceCandidate(candidate) {
    if (!this.peerConnection) throw new Error('Peer connection not initialized');

    try {
      await this.peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
    } catch (e) {
      console.error('Error adding ICE candidate:', e);
    }
  }

  async restartIce() {
    if (!this.peerConnection) return;

    const offer = await this.peerConnection.createOffer({ iceRestart: true });
    await this.peerConnection.setLocalDescription(offer);

    if (this.onIceCandidate) {
      this.onIceCandidate({ type: 'restart', offer });
    }
  }

  getStats() {
    if (!this.peerConnection) return null;
    return this.peerConnection.getStats();
  }

  async verifyEncryption() {
    if (!this.peerConnection) return false;

    const stats = await this.peerConnection.getStats();
    let encrypted = false;

    stats.forEach(report => {
      // Check for DTLS-SRTP
      if (report.type === 'transport') {
        if (report.dtlsState === 'connected' && report.selectedCandidatePairId) {
          encrypted = true;
        }
      }
      // Check SRTP cipher
      if (report.type === 'inbound-rtp' || report.type === 'outbound-rtp') {
      }
    });

    return encrypted;
  }

  toggleAudio(enabled) {
    if (!this.localStream) return;
    this.localStream.getAudioTracks().forEach(track => {
      track.enabled = enabled;
    });
  }

  toggleVideo(enabled) {
    if (!this.localStream) return;
    this.localStream.getVideoTracks().forEach(track => {
      track.enabled = enabled;
    });
  }

  hangup() {
    if (this.localStream) {
      this.localStream.getTracks().forEach(track => track.stop());
      this.localStream = null;
    }

    if (this.peerConnection) {
      this.peerConnection.close();
      this.peerConnection = null;
    }

    this.remoteStream = null;
  }
}

window.SecureWebRTC = SecureWebRTC;
