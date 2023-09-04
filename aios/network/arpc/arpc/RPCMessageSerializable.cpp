/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "aios/network/arpc/arpc/RPCMessageSerializable.h"

#include <google/protobuf/descriptor.h>
#include <stddef.h>

#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/arpc/arpc/DataBufferOutputStream.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

using namespace anet;
using namespace google::protobuf::io;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(RPCMessageSerializable);

RPCMessageSerializable::RPCMessageSerializable(RPCMessage *header,
                                               RPCMessage *body,
                                               const std::shared_ptr<google::protobuf::Arena> &arena) {
    _arena = arena;
    _header = header;
    _body = body;
}

RPCMessageSerializable::~RPCMessageSerializable() {}

bool RPCMessageSerializable::serialize(DataBuffer *outputBuffer) const {
    if (_header == NULL || _body == NULL || outputBuffer == NULL) {
        return false;
    }

    if (!serializeMessage(_header, outputBuffer)) {
        ARPC_LOG(ERROR, "serialize header error");
        return false;
    }

    if (!serializeMessage(_body, outputBuffer)) {
        ARPC_LOG(ERROR, "serialize body error");
        return false;
    }

    return true;
}

bool RPCMessageSerializable::deserialize(anet::DataBuffer *inputBuffer, int length) {
    if (length == 0) {
        return true;
    }

    if (_header == NULL || _body == NULL || inputBuffer == NULL) {
        return false;
    }

    int leftPacketDataLen = length;

    if (!deserializeMessage(inputBuffer, _header, leftPacketDataLen)) {
        inputBuffer->drainData(leftPacketDataLen);
        ARPC_LOG(ERROR, "deserialize header error");
        return false;
    }

    if (!deserializeMessage(inputBuffer, _body, leftPacketDataLen)) {
        ARPC_LOG(ERROR, "deserialize body error");
        inputBuffer->drainData(leftPacketDataLen);
        return false;
    }

    return true;
}

bool RPCMessageSerializable::serializeMessage(const RPCMessage *message, DataBuffer *outputBuffer) {
    int oldLen = outputBuffer->getDataLen();
    int size = message->ByteSize();
    if (size < 0 || size > MAX_RPC_MSG_BYTE_SIZE) {
        ARPC_LOG(
            ERROR, "failed to serialize message. ByteSize %d should be between 0 and %d", size, MAX_RPC_MSG_BYTE_SIZE);
        return false;
    }

    outputBuffer->writeInt32(0);
    outputBuffer->ensureFree(size);
    DataBufferOutputStream outputStream(outputBuffer);

    bool ret = message->IsInitialized();

    if (!ret) {
        ARPC_LOG(ERROR,
                 "failed to serialize message: required fields"
                 " may not be initialized");
        return false;
    }

    ret = message->SerializeToZeroCopyStream(&outputStream);
    int appendLen = outputBuffer->getDataLen() - oldLen;

    if (!ret) {
        outputBuffer->stripData(appendLen);
        ARPC_LOG(ERROR, "serialize the message failed");
        return false;
    }

    unsigned char *ptr = (unsigned char *)(outputBuffer->getData() + oldLen);
    outputBuffer->fillInt32(ptr, appendLen - sizeof(int32_t));
    return true;
}

bool RPCMessageSerializable::deserializeMessage(DataBuffer *inputBuffer,
                                                RPCMessage *message,
                                                int32_t &leftPacketDataLen) {
    int32_t len = inputBuffer->readInt32();
    leftPacketDataLen -= sizeof(int32_t);

    if (len == 0 && leftPacketDataLen == 0) {
        // All fileds in current message are optional
        return true;
    }

    bool ret = false;

    if (len > 0 && len <= leftPacketDataLen) {
        char *pData = inputBuffer->getData();
        google::protobuf::io::CodedInputStream input((uint8_t *)(pData), len);
        input.SetTotalBytesLimit(MAX_RPC_MSG_BYTE_SIZE, MAX_RPC_MSG_BYTE_SIZE);
        ret = (message->ParseFromCodedStream(&input) && input.ConsumedEntireMessage());
        inputBuffer->drainData(len);
        leftPacketDataLen -= len;
    }

    return ret;
}

TYPEDEF_PTR(RPCMessageSerializable);
ARPC_END_NAMESPACE(arpc);
