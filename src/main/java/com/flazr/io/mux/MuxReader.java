package com.flazr.io.mux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jcodec.codecs.h264.H264Utils;

import com.flazr.io.flv.AudioTag;
import com.flazr.io.flv.AudioTag.SampleRate;
import com.flazr.io.flv.VideoTag;
import com.flazr.io.flv.VideoTag.FrameType;
import com.flazr.rtmp.RtmpMessage;
import com.flazr.rtmp.RtmpReader;
import com.flazr.rtmp.message.Audio;
import com.flazr.rtmp.message.MessageType;
import com.flazr.rtmp.message.Metadata;
import com.flazr.rtmp.message.MetadataAmf0;
import com.flazr.rtmp.message.Video;

public class MuxReader implements RtmpReader {

	private int width;
	private int height;

	private VideoTag.CodecType videoCodec;

	private AudioTag.CodecType audioCodec;
	private AudioTag.SampleRate sampleRate = SampleRate.KHZ_44;
	private boolean audio16Bit = true;
	private boolean stereo = true;

	private MetadataAmf0 metadata;
	private long lastTime;

	private BlockingQueue<RtmpMessage> queue = new LinkedBlockingDeque<RtmpMessage>(100);
	private int soundByte;

	private boolean closed;

	public void start() {
		metadata = new MetadataAmf0("onMetaData");
		metadata.setValue("width", width);
		metadata.setValue("height", height);
		if (audioCodec != null)
			metadata.setValue("audiocodecid", audioCodec.intValue());
		if (videoCodec != null)
			metadata.setValue("videocodecid", videoCodec.intValue());
	}

	public void sendVideo(long timestamp, ByteBuffer buffer, int offset, int length) throws IOException {
		if (closed) {
			throw new IOException("stream closed");
		} 
		if (videoCodec == null) {
			throw new IOException("no audio");			
		}
		lastTime = timestamp;
        // int length = buffer.remaining();
		final ChannelBuffer out = ChannelBuffers.buffer(20 + length);
        out.writeByte((byte) MessageType.VIDEO.intValue());
        out.writeMedium(length);
        out.writeMedium((int) timestamp);
        out.writeByte((int) ((timestamp>>24) & 0xff)); // 4 bytes of zeros (reserved)
        out.writeMedium(0);
        VideoTag.FrameType frameType = H264Utils.idrSlice(buffer) ? FrameType.KEY : FrameType.INTER; 
        out.writeByte(frameType.intValue() << 4 | videoCodec.intValue());
        out.writeInt(0); // AVCPacketType, CompositionTime
        out.writeBytes(buffer.array(), offset, length);
        out.writeInt(length + 16); // previous tag size
        Video msg = new Video((int)timestamp, out);
        if (queue.offer(msg)) {
        	throw new IOException("buffers full");
        }
	}

	public void sendAudio(long timestamp,  ByteBuffer buffer, int offset, int length) throws IOException {
		if (closed) {
			throw new IOException("stream closed");
		}
		if (audioCodec == null) {
			throw new IOException("no audio");
		}
		lastTime = timestamp;
        // int length = buffer.remaining();
		final ChannelBuffer out = ChannelBuffers.buffer(16 + length);
		out.writeByte((byte) MessageType.AUDIO.intValue());
		out.writeMedium(length);
		out.writeMedium((int) timestamp);
		out.writeByte((int) ((timestamp >> 24) & 0xff)); // 4 bytes of zeros
															// (reserved)
		out.writeMedium(0);
		out.writeByte(soundByte);
        out.writeBytes(buffer.array(), offset, length);
		out.writeInt(length + 12); // previous tag size
		Audio msg = new Audio((int) timestamp, out);
		if (queue.offer(msg)) {
			throw new IOException("buffers full");
		}
	}

	@Override
	public Metadata getMetadata() {
		return metadata;
	}

	@Override
	public RtmpMessage[] getStartMessages() {
		return new RtmpMessage[] { metadata };
	}

	@Override
	public void setAggregateDuration(int targetDuration) {
		// FIXME: ignored...
	}

	@Override
	public long getTimePosition() {
		return lastTime;
	}

	@Override
	public long seek(long timePosition) {
		return lastTime;
	}

	@Override
	public void close() {

	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public RtmpMessage next() {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			return null;
		}
	}

	@Override
	public int getWidth() {
		return width;
	}

	@Override
	public int getHeight() {
		return height;
	}

	private void calcSoundByte() {
		soundByte = (audioCodec.intValue() << 4) | (sampleRate.intValue() << 2)
				| (audio16Bit ? 1 : 0) | (stereo ? 1 : 0);
	}

	public VideoTag.CodecType getVideoCodec() {
		return videoCodec;
	}

	public void setVideoCodec(VideoTag.CodecType videoCodec) {
		this.videoCodec = videoCodec;
	}

	public AudioTag.CodecType getAudioCodec() {
		return audioCodec;
	}

	public void setAudioCodec(AudioTag.CodecType audioCodec) {
		this.audioCodec = audioCodec;
		calcSoundByte();
	}

	public AudioTag.SampleRate getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(AudioTag.SampleRate sampleRate) {
		this.sampleRate = sampleRate;
		calcSoundByte();
	}

	public boolean isAudio16Bit() {
		return audio16Bit;
	}

	public void setAudio16Bit(boolean audio16Bit) {
		this.audio16Bit = audio16Bit;
		calcSoundByte();
	}

	public boolean isStereo() {
		return stereo;
	}

	public void setStereo(boolean stereo) {
		this.stereo = stereo;
		calcSoundByte();
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public void setHeight(int height) {
		this.height = height;
	}
}
