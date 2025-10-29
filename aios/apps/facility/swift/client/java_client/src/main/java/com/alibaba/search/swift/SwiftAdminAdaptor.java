package com.alibaba.search.swift;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import com.alibaba.search.swift.library.SwiftClientApiLibrary;
import com.alibaba.search.swift.protocol.Common;
import com.alibaba.search.swift.protocol.AdminRequestResponse;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.exception.SwiftException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.PointerByReference;

public class SwiftAdminAdaptor {


    private NativeLong _adminPtr;

    public SwiftAdminAdaptor(NativeLong adminPtr) {
        _adminPtr = adminPtr;
    }

    public NativeLong getAdminPtr() {
        return _adminPtr;
    }

    public synchronized String getBrokerAddress(String topicName, int partitionId) throws SwiftException {
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN, "");
        }
        PointerByReference addStr = new PointerByReference();
        IntBuffer strLen = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.getBrokerAddress(_adminPtr, topicName, partitionId, addStr,
                strLen);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
        String brokerAddres = new String(addStr.getValue().getByteArray(0, strLen.get(0)));
        SwiftClientApiLibrary.freeString(addStr.getValue());
        return brokerAddres;
    }

    public synchronized void createTopic(AdminRequestResponse.TopicCreationRequest request) throws SwiftException {
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(request.toByteArray());
        int retEc = SwiftClientApiLibrary.createTopic(_adminPtr, byteBuffer, byteBuffer.capacity());
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized void deleteTopic(String topicName) throws SwiftException {
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        int retEc = SwiftClientApiLibrary.deleteTopic(_adminPtr, topicName);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
    }

    public synchronized boolean waitTopicReady(String topicName, int timeout) throws SwiftException {
        long stopTime = System.currentTimeMillis() + timeout * 1000;
        while (System.currentTimeMillis() < stopTime) {
            try {
                AdminRequestResponse.TopicInfoResponse response = getTopicInfo(topicName);
                Common.TopicInfo topicInfo = response.getTopicInfo();
                Common.TopicStatus status = topicInfo.getStatus();
                if (status == Common.TopicStatus.TOPIC_STATUS_RUNNING) {
                    return true;
                }
            } catch (SwiftException e) {
                if (ErrCode.ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED != e.getEc()) {
                    throw e;
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
        return false;
    }

    public synchronized AdminRequestResponse.TopicInfoResponse getTopicInfo(String topicName) throws SwiftException {
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        PointerByReference infoStr = new PointerByReference();
        IntBuffer strLen = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.getTopicInfo(_adminPtr, topicName, infoStr, strLen);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
        AdminRequestResponse.TopicInfoResponse response = null;
        try {
            byte[] byteStr = infoStr.getValue().getByteArray(0, strLen.get(0));
            response = AdminRequestResponse.TopicInfoResponse.parseFrom(byteStr);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN, "InvalidProtocolBufferException");
        } finally {
            SwiftClientApiLibrary.freeString(infoStr.getValue());
        }
        return response;
    }

    public synchronized int getPartitionCount(String topicName) throws SwiftException {
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        IntBuffer pcount = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.getPartitionCount(_adminPtr, topicName, pcount);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
        return pcount.get(0);
    }


    public synchronized AdminRequestResponse.PartitionInfoResponse getPartitionInfo(String topicName, int partitionId) throws SwiftException {
        AdminRequestResponse.PartitionInfoResponse partInfo = null;
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN, "");
        }
        PointerByReference infoStr = new PointerByReference();
        IntBuffer strLen = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.getPartitionInfo(_adminPtr, topicName, partitionId, infoStr,
                strLen);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
        try {
            byte[] byteStr = infoStr.getValue().getByteArray(0, strLen.get(0));
            partInfo = AdminRequestResponse.PartitionInfoResponse.parseFrom(byteStr);
            return partInfo;
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN, "InvalidProtocolBufferException");
        } finally {
            SwiftClientApiLibrary.freeString(infoStr.getValue());
        }
    }

    public synchronized AdminRequestResponse.AllTopicInfoResponse getAllTopicInfo() throws SwiftException {
        if (_adminPtr.longValue() == 0) {
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        }
        PointerByReference infoStr = new PointerByReference();
        IntBuffer strLen = IntBuffer.allocate(1);
        int retEc = SwiftClientApiLibrary.getAllTopicInfo(_adminPtr, infoStr, strLen);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
        byte[] byteStr = infoStr.getValue().getByteArray(0, strLen.get(0));
        AdminRequestResponse.AllTopicInfoResponse alltopicInfos = null;
        try {
            alltopicInfos = AdminRequestResponse.AllTopicInfoResponse.parseFrom(byteStr);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN);
        } finally {
            SwiftClientApiLibrary.freeString(infoStr.getValue());
        }
        return alltopicInfos;
    }

    public synchronized boolean isClosed() {
        return _adminPtr.longValue() == 0;
    }
    
    public synchronized void close() {
        _adminPtr.setValue(0);
    }

    protected void finalize() {
        close();
    }
}
