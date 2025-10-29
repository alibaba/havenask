package com.alibaba.search.swift;

import java.nio.IntBuffer;
import java.nio.LongBuffer;

import com.alibaba.search.swift.library.SwiftClientApiLibrary;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.protocol.SwiftMessage;
import com.alibaba.search.swift.exception.SwiftException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;

public class SwiftReader {

    private NativeLong _readerPtr;

    public SwiftReader(NativeLong readerPtr) {
        _readerPtr = readerPtr;
    }

    public synchronized NativeLong getReaderPtr() {
        return _readerPtr;
    }

    public synchronized SwiftMessage.Message read(LongBuffer timeStamp, long timeout) throws SwiftException {
        if (_readerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        NativeLongByReference timestampRef = new NativeLongByReference();
        PointerByReference msgRef = new PointerByReference();
        IntBuffer intBuf = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.readMessage(_readerPtr, timestampRef, msgRef, intBuf,
                new NativeLong(timeout));
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }

        timeStamp.put(timestampRef.getValue().longValue());
        int msgLen = intBuf.get(0);
        byte[] byteArray = msgRef.getValue().getByteArray(0, msgLen);
        SwiftMessage.Message msg = null;
        try {
            msg = SwiftMessage.Message.parseFrom(byteArray);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        } finally {
            SwiftClientApiLibrary.freeString(msgRef.getValue());
        }
        return msg;
    }

    public SwiftMessage.Message read(LongBuffer timeStamp) throws SwiftException {
        return read(timeStamp, 3 * 1000 * 1000);
    }

    public SwiftMessage.Message read() throws SwiftException {
        return read(LongBuffer.allocate(1), 3 * 1000 * 1000);
    }

    public synchronized SwiftMessage.Messages reads(LongBuffer timeStamp, long timeout) throws SwiftException {
        if (_readerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        NativeLongByReference timestampRef = new NativeLongByReference();
        PointerByReference msgRef = new PointerByReference();
        IntBuffer intBuf = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.readMessages(_readerPtr, timestampRef, msgRef, intBuf,
                new NativeLong(timeout));
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }

        timeStamp.put(timestampRef.getValue().longValue());
        int msgLen = intBuf.get(0);
        byte[] byteArray = msgRef.getValue().getByteArray(0, msgLen);
        SwiftMessage.Messages msgs = null;
        try {
            msgs = SwiftMessage.Messages.parseFrom(byteArray);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        } finally {
            SwiftClientApiLibrary.freeString(msgRef.getValue());
        }
        return msgs;
    }

    public SwiftMessage.Messages reads(LongBuffer timeStamp) throws SwiftException {
        return reads(timeStamp, 3 * 1000 * 1000);
    }

    public SwiftMessage.Messages reads() throws SwiftException {
        return reads(LongBuffer.allocate(1), 3 * 1000 * 1000);
    }

    public synchronized void seekByTimestamp(Long timeStamp, boolean force) throws SwiftException {
        if (_readerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        byte f = (byte) (force ? 1 : 0);
        int retEc = SwiftClientApiLibrary.seekByTimestamp(_readerPtr, new NativeLong(timeStamp), f);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized void seekByMessageId(Long msgId) throws SwiftException {
        if (_readerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        int retEc = SwiftClientApiLibrary.seekByMessageId(_readerPtr, new NativeLong(msgId));
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized void setTimestampLimit(long timeLimit, LongBuffer acceptTimestamp) throws SwiftException {
        if (_readerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        NativeLongByReference longRef = new NativeLongByReference();
        SwiftClientApiLibrary.setTimestampLimit(_readerPtr, new NativeLong(timeLimit), longRef);
        acceptTimestamp.put(longRef.getValue().longValue());
    }

    public synchronized void getPartitionStatus(LongBuffer refreshTime, LongBuffer maxMessageId,
                                   LongBuffer maxMessageTimestamp) throws SwiftException{
        if (_readerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        NativeLongByReference refTime = new NativeLongByReference();
        NativeLongByReference maxId = new NativeLongByReference();
        NativeLongByReference maxTime = new NativeLongByReference();
        SwiftClientApiLibrary.getPartitionStatus(_readerPtr, refTime, maxId, maxTime);
        refreshTime.put(refTime.getValue().longValue());
        maxMessageId.put(maxId.getValue().longValue());
        maxMessageTimestamp.put(maxTime.getValue().longValue());
    }

    public synchronized boolean isClosed() {
        return _readerPtr.longValue() == 0;
    }

    public synchronized void close() {
        if (_readerPtr.longValue() == 0) {
            return;
        }
        SwiftClientApiLibrary.deleteSwiftReader(_readerPtr);
        _readerPtr.setValue(0);
    }

    protected void finalize() {
        close();
    }
}
