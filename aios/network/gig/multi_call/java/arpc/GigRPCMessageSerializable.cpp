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
#include "aios/network/gig/multi_call/java/arpc/GigRPCMessageSerializable.h"
#include "aios/network/arpc/arpc/RPCMessageSerializable.h"

using namespace std;
using namespace arpc;
using namespace anet;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigRPCMessageSerializable);

GigRPCMessageSerializable::GigRPCMessageSerializable(RPCMessage *header,
                                                     std::string *body)
    : _header(header), _body(body) {}

GigRPCMessageSerializable::~GigRPCMessageSerializable() {}

bool GigRPCMessageSerializable::serialize(
    anet::DataBuffer *outputBuffer) const {
    if (_header == NULL || _body == NULL || outputBuffer == NULL) {
        return false;
    }
    if (!RPCMessageSerializable::serializeMessage(_header, outputBuffer)) {
        AUTIL_LOG(ERROR, "serialize header error");
        return false;
    }

    if (!serializeStringMessage(_body, outputBuffer)) {
        AUTIL_LOG(ERROR, "serialize body error");
        return false;
    }

    return true;
}

bool GigRPCMessageSerializable::deserialize(anet::DataBuffer *inputBuffer,
                                            int length) {
    if (length == 0) {
        return true;
    }

    if (_header == NULL || _body == NULL || inputBuffer == NULL) {
        return false;
    }

    int leftPacketDataLen = length;

    if (!RPCMessageSerializable::deserializeMessage(inputBuffer, _header,
                                                    leftPacketDataLen)) {
        inputBuffer->drainData(leftPacketDataLen);
        AUTIL_LOG(ERROR, "deserialize header error");
        return false;
    }

    if (!deserializeStringMessage(inputBuffer, _body, leftPacketDataLen)) {
        AUTIL_LOG(ERROR, "deserialize body error");
        inputBuffer->drainData(leftPacketDataLen);
        return false;
    }

    return true;
}

bool GigRPCMessageSerializable::serializeStringMessage(
    const std::string *message, DataBuffer *outputBuffer) const {
    int oldLen = outputBuffer->getDataLen();
    int size = message->length();
    if (size < 0 || size > MAX_RPC_MSG_BYTE_SIZE) {
        AUTIL_LOG(ERROR,
                  "failed to serialize message. ByteSize %d should be between "
                  "0 and %d",
                  size, MAX_RPC_MSG_BYTE_SIZE);
        return false;
    }

    outputBuffer->writeInt32(0);
    outputBuffer->ensureFree(size);
    outputBuffer->writeBytes(message->c_str(), size);
    int appendLen = outputBuffer->getDataLen() - oldLen;

    unsigned char *ptr = (unsigned char *)(outputBuffer->getData() + oldLen);
    outputBuffer->fillInt32(ptr, appendLen - sizeof(int32_t));
    return true;
}

bool GigRPCMessageSerializable::deserializeStringMessage(
    anet::DataBuffer *inputBuffer, std::string *message,
    int32_t &leftPacketDataLen) {
    int32_t len = inputBuffer->readInt32();
    leftPacketDataLen -= sizeof(int32_t);

    if (len == 0 && leftPacketDataLen == 0) {
        // All fileds in current message are optional
        return true;
    }

    if (len > 0 && len <= leftPacketDataLen) {
        char *pData = inputBuffer->getData();
        message->assign(pData, len);
        inputBuffer->drainData(len);
        leftPacketDataLen -= len;
    }

    return true;
}

} // namespace multi_call
