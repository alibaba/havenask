package com.alibaba.search.swift.util;

import java.util.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.ErrCode;

public class FieldGroupReader {
	private int readCursor;
	private int dataSize;
	private byte[] bytes;
	Vector<Field> fieldVec;
	
	public FieldGroupReader() {
		readCursor = 0;
		dataSize = 0;
		fieldVec = new Vector<Field>();
	}
	
	public void reset(byte[] bytes) {	
		this.bytes = bytes;
		dataSize = bytes.length;
		readCursor = 0;
		fieldVec.clear();
	}
	
	public int getFieldSize() {
		return fieldVec.size();
	}
	
	public Field getField(int index) {
		return fieldVec.get(index);
	}
	
	public boolean fromProductionString(byte[] bytes) throws IOException, SwiftException {
		reset(bytes);
		int len = 0;
		
		while (readCursor < dataSize) {
			Field tmpField = new Field();
			len = readRawVarint32();
			tmpField.name = readBytes(len);
			len = readRawVarint32();
			tmpField.value = readBytes(len);
			tmpField.isUpdated = readBool();
			fieldVec.addElement(tmpField);
		}
		return true;
	}

    public boolean fromConsumptionString(byte[] bytes) throws IOException, SwiftException {
        reset(bytes);
        int len = 0;
        while (readCursor < dataSize) {
            Field tmpField = new Field();
            tmpField.isExisted = readBool();
            if (!tmpField.isExisted) {
                fieldVec.addElement(tmpField);
            } else {
                len = readRawVarint32();
                tmpField.value = readBytes(len);
                tmpField.isUpdated = readBool();
                fieldVec.addElement(tmpField);
            }
        }
        return true;
    }
	
	private String readBytes(int len) throws SwiftException, UnsupportedEncodingException {
		if ((readCursor + len) > dataSize) {
			throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
		}
		String str = new String(bytes, readCursor, len, "UTF-8");
		readCursor += len;
		return str;
	}
	
	/** Read a {@code bool} field value from the stream. */
	private boolean readBool() throws IOException, SwiftException {
		return readRawVarint32() != 0;
	}

	public int readRawVarint32() throws SwiftException {
		byte tmp = readRawByte();
		if (tmp >= 0) {
			return tmp;
		}
		int result = tmp & 0x7f;
		if ((tmp = readRawByte()) >= 0) {
			result |= tmp << 7;
		} else {
			result |= (tmp & 0x7f) << 7;
			if ((tmp = readRawByte()) >= 0) {
				result |= tmp << 14;
			} else {
				result |= (tmp & 0x7f) << 14;
				if ((tmp = readRawByte()) >= 0) {
					result |= tmp << 21;
				} else {
					result |= (tmp & 0x7f) << 21;
					result |= (tmp = readRawByte()) << 28;
					if (tmp < 0) {
						// Discard upper 32 bits.
						for (int i = 0; i < 5; i++) {
							if (readRawByte() >= 0) {
								return result;
							}
						}
						throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
					}
				}
			}
		}
		return result;
	}
	
	private byte readRawByte() throws SwiftException {
		if (readCursor >= dataSize) {
			throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
		}
		return bytes[readCursor++];
	}
}
