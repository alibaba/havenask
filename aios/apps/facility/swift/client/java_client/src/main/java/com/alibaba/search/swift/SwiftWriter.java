package com.alibaba.search.swift;

import java.nio.ByteBuffer;

import com.alibaba.search.swift.library.SwiftClientApiLibrary;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.protocol.SwiftMessage;
import com.alibaba.search.swift.exception.SwiftException;
import com.sun.jna.NativeLong;

public class SwiftWriter {
    private NativeLong _writerPtr;

    public SwiftWriter(NativeLong writerPtr) {
        _writerPtr = writerPtr;
    }

    public synchronized NativeLong getWriterPtr() {
        return _writerPtr;
    }

    public synchronized void write(SwiftMessage.WriteMessageInfo msg) throws SwiftException {
        if (_writerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(msg.toByteArray());
        int retEc = SwiftClientApiLibrary.writeMessage(_writerPtr, byteBuffer, byteBuffer.capacity());
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized void write(SwiftMessage.WriteMessageInfoVec msgVec, boolean waitSent) throws SwiftException {
        if (_writerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(msgVec.toByteArray());
        byte w = (byte) (waitSent ? 1 : 0);
        int retEc = SwiftClientApiLibrary.writeMessages(_writerPtr, byteBuffer, byteBuffer.capacity(), w);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized long getCommittedCheckpointId() throws SwiftException {
        if (_writerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        NativeLong retLong = SwiftClientApiLibrary.getCommittedCheckpointId(_writerPtr);
        return retLong.longValue();
    }

    public synchronized void waitFinished(long timeout) throws SwiftException {
        if (_writerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        int retEc = SwiftClientApiLibrary.waitFinished(_writerPtr, new NativeLong(timeout));
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized void waitSent(long timeout) throws SwiftException {
        if (_writerPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        int retEc = SwiftClientApiLibrary.waitSent(_writerPtr, new NativeLong(timeout));
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public void waitFinished() throws SwiftException {
        waitFinished(30 * 1000 * 1000);
    }

    public void waitSent() throws SwiftException {
        waitSent(3 * 1000 * 1000);
    }

    public synchronized boolean isClosed() {
        return _writerPtr.longValue() == 0;
    }

    public synchronized void close() {
        if (_writerPtr.longValue() == 0) {
            return;
        }
        SwiftClientApiLibrary.deleteSwiftWriter(_writerPtr);
        _writerPtr.setValue(0);
    }

    protected void finalize() {
        close();
    }
}
