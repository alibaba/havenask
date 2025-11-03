package com.alibaba.search.swift.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FieldGroupWriter {
	private byte[] buffer;
	private int limit;
	private int position;
	
	private ByteArrayOutputStream output;
	private final int BUFFERSIZE = 1024;
	
	public FieldGroupWriter() {
		output = new ByteArrayOutputStream(); 
		buffer = new byte[BUFFERSIZE];
		position = 0;
		limit = buffer.length;
	}

	public byte[] toBytes() throws IOException {		
		refreshBuffer();
		return output.toByteArray();
	}
	
	public void reset() throws IOException {		
		position = 0;
		output.reset();
	}
	
	/** Write a {@code bytes} field to the stream. */
	public void addProductionField(final byte[] name, final byte[] value, final boolean isUpdated) throws IOException {	
		writeBytes(name);
		writeBytes(value);
		writeBool(isUpdated);
	}
	
	/** Write a {@code bytes} field to the stream. */
	private void writeBytes(final byte[] value) throws IOException {		
	    writeRawVarint32(value.length);
	    writeRawBytes(value, 0, value.length);
	}
	
	/** Write a {@code bytes} field to the stream. */
	public void addProductionField(final String name, final String value, final boolean isUpdated) throws IOException {	
		writeBytes(name);
		writeBytes(value);
		writeBool(isUpdated);
	}
	
	/** Write a {@code bytes} field to the stream. */
	private void writeBytes(final String value) throws IOException {		
		byte[] bt =  value.getBytes("UTF-8");
	    writeRawVarint32(bt.length);
	    writeRawBytes(bt, 0, bt.length);
	}
		
	/** Write part of an array of bytes. */
	private void writeRawBytes(final byte[] value, int offset, int length)
			throws IOException {
		if (limit - position >= length) {
			// We have room in the current buffer.
			System.arraycopy(value, offset, buffer, position, length);
			position += length;
		} else {
			// Write extends past current buffer. Fill the rest of this buffer
			// and
			// flush.
			final int bytesWritten = limit - position;
			System.arraycopy(value, offset, buffer, position, bytesWritten);
			offset += bytesWritten;
			length -= bytesWritten;
			position = limit;
			refreshBuffer();

			// Now deal with the rest.
			// Since we have an output stream, this is our buffer
			// and buffer offset == 0
			if (length <= limit) {
				// Fits in new buffer.
				System.arraycopy(value, offset, buffer, 0, length);
				position = length;
			} else {
				// Write is very big. Let's do it all at once.
				output.write(value, offset, length);
			}
		}
	}

	
	/** Write a {@code bool} field to the stream. */
	private void writeBool(final boolean value) throws IOException {
		writeRawByte(value ? 1 : 0);
	}
	  
	/**
	 * Encode and write a varint. {@code value} is treated as unsigned, so it
	 * won't be sign-extended if negative.
	 */
	public void writeRawVarint32(int value) throws IOException {
		while (true) {
			if ((value & ~0x7F) == 0) {
				writeRawByte(value);
				return;
			} else {
				writeRawByte((value & 0x7F) | 0x80);
				value >>>= 7;
			}
		}
	}

	/** Write a single byte, represented by an integer value. */
	private void writeRawByte(final int value) throws IOException {
		writeRawByte((byte) value);
	}
	
	/** Write a single byte. */
	private void writeRawByte(final byte value) throws IOException {
		if (position == limit) {
			refreshBuffer();
		}

		buffer[position++] = value;
	}

	private void refreshBuffer() {
		// Since we have an output stream, this is our buffer
		// and buffer offset == 0
		output.write(buffer, 0, position);
		position = 0;
	}
}
