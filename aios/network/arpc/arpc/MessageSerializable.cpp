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
#include "aios/network/arpc/arpc/MessageSerializable.h"

#include <cassert>
#include <cstddef>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <stdint.h>

#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/DataBufferOutputStream.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

using namespace std;
using namespace anet;
using namespace google::protobuf::io;

ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(MessageSerializable);

MessageSerializable::MessageSerializable(RPCMessage *message, const std::shared_ptr<google::protobuf::Arena> &arena) {
    _arena = arena;
    _message = message;
    if (_message) {
        _messageSerializedSize = _message->ByteSizeLong();
    }
}

MessageSerializable::~MessageSerializable() {}

bool MessageSerializable::serialize(DataBuffer *outputBuffer) const {
    if (_message == NULL) {
        return true;
    }

    if (!_message->IsInitialized()) {
        ARPC_LOG(ERROR,
                 "failed to serialize: required fields may "
                 "not be initialized");
        return false;
    }

    if (_messageSerializedSize > MAX_RPC_MSG_BYTE_SIZE) {
        ARPC_LOG(ERROR,
                 "failed to serialize message. ByteSize %lu should be between 0 and %d",
                 _messageSerializedSize,
                 MAX_RPC_MSG_BYTE_SIZE);
        return false;
    }
    outputBuffer->ensureFree(_messageSerializedSize);
    DataBufferOutputStream outputStream(outputBuffer);
    bool ret = _message->SerializeToZeroCopyStream(&outputStream);

    return ret;
}

bool MessageSerializable::deserialize(DataBuffer *inputBuffer, int length) {
    if (length == 0) {
        return true;
    }

    char *pData = inputBuffer->getData();
    google::protobuf::io::CodedInputStream input((uint8_t *)(pData), length);
    input.SetTotalBytesLimit(MAX_RPC_MSG_BYTE_SIZE, MAX_RPC_MSG_BYTE_SIZE);
    bool ret = (_message->ParseFromCodedStream(&input) && input.ConsumedEntireMessage());
    inputBuffer->drainData(length);

    return ret;
}

ARPC_END_NAMESPACE(arpc);
