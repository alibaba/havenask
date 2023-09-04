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
#include "swift/protocol/MessageCompressor.h"

#include <cstddef>

#include "autil/TimeUtility.h"
#include "swift/common/MessageInfo.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"

using namespace std;
using namespace autil;
using namespace swift::common;

namespace swift {
namespace protocol {
AUTIL_LOG_SETUP(swift, MessageCompressor);

MessageCompressor::MessageCompressor() {}

MessageCompressor::~MessageCompressor() {}

void MessageCompressor::compressProductionMessage(ProductionRequest *request, uint64_t compressThresholdInBytes) {
    float ratio;
    MessageCompressor::compressMessage<ProductionRequest>(request, compressThresholdInBytes, ratio);
}

void MessageCompressor::compressProductionMessage(ProductionRequest *request,
                                                  uint64_t compressThresholdInBytes,
                                                  float &ratio) {
    MessageCompressor::compressMessage<ProductionRequest>(request, compressThresholdInBytes, ratio);
}

ErrorCode MessageCompressor::decompressProductionMessage(ProductionRequest *request) {
    float ratio;
    return MessageCompressor::decompressMessage<ProductionRequest>(request, ratio, true);
}
ErrorCode MessageCompressor::decompressProductionMessage(ProductionRequest *request, float &ratio) {
    return MessageCompressor::decompressMessage<ProductionRequest>(request, ratio, true);
}

void MessageCompressor::compressResponseMessage(MessageResponse *response) {
    float ratio;
    MessageCompressor::compressMessage<MessageResponse>(response, 0, ratio);
}

void MessageCompressor::compressResponseMessage(MessageResponse *response, float &ratio) {
    MessageCompressor::compressMessage<MessageResponse>(response, 0, ratio);
}

ErrorCode MessageCompressor::decompressResponseMessage(MessageResponse *response, bool decompMerge) {
    float ratio;
    return MessageCompressor::decompressMessage<MessageResponse>(response, ratio, decompMerge);
}

ErrorCode MessageCompressor::decompressResponseMessage(MessageResponse *response, float &ratio, bool decompMerge) {
    return MessageCompressor::decompressMessage<MessageResponse>(response, ratio, decompMerge);
}

void MessageCompressor::compressMessageResponse(MessageResponse *response) {
    float ratio;
    MessageCompressor::compress<MessageResponse>(response, ratio);
}

void MessageCompressor::compressMessageResponse(MessageResponse *response, float &ratio) {
    MessageCompressor::compress<MessageResponse>(response, ratio);
}

ErrorCode MessageCompressor::decompressMessageResponse(MessageResponse *response) {
    float ratio;
    return MessageCompressor::decompress<MessageResponse>(response, ratio);
}

ErrorCode MessageCompressor::decompressMessageResponse(MessageResponse *response, float &ratio) {
    return MessageCompressor::decompress<MessageResponse>(response, ratio);
}

void MessageCompressor::compressProductionRequest(ProductionRequest *request) {
    float ratio;
    MessageCompressor::compress<ProductionRequest>(request, ratio);
}

void MessageCompressor::compressProductionRequest(ProductionRequest *request, float &ratio) {
    MessageCompressor::compress<ProductionRequest>(request, ratio);
}

ErrorCode MessageCompressor::decompressProductionRequest(ProductionRequest *request) {
    float ratio;
    return MessageCompressor::decompress<ProductionRequest>(request, ratio);
}

ErrorCode MessageCompressor::decompressProductionRequest(ProductionRequest *request, float &ratio) {
    return MessageCompressor::decompress<ProductionRequest>(request, ratio);
}

bool MessageCompressor::compressData(autil::ZlibCompressor *compressor,
                                     const char *inputData,
                                     size_t dataLen,
                                     string &compressData) {
    compressor->reset();
    compressor->addDataToBufferIn(inputData, dataLen);
    if (!compressor->compress()) {
        return false;
    }
    compressData.append(compressor->getBufferOut(), compressor->getBufferOutLen());
    return true;
}
bool MessageCompressor::compressData(autil::ZlibCompressor *compressor, const string &inputData, string &compressData) {
    return MessageCompressor::compressData(compressor, inputData.c_str(), inputData.size(), compressData);
}

bool MessageCompressor::uncompressData(autil::ZlibCompressor *compressor,
                                       const string &inputData,
                                       string &uncompressData) {
    return MessageCompressor::uncompressData(compressor, inputData.c_str(), inputData.size(), uncompressData);
}

bool MessageCompressor::uncompressData(autil::ZlibCompressor *compressor,
                                       const char *inputData,
                                       size_t len,
                                       string &uncompressData) {
    compressor->reset();
    compressor->addDataToBufferIn(inputData, len);
    if (!compressor->decompress()) {
        return false;
    }
    uncompressData.append(compressor->getBufferOut(), compressor->getBufferOutLen());
    return true;
}

bool MessageCompressor::compressMergedMessage(autil::ZlibCompressor *compressor,
                                              uint64_t compressThreshold,
                                              const char *data,
                                              size_t dataLen,
                                              string &compressedData) {
    if (dataLen <= sizeof(uint16_t)) {
        return false;
    }
    compressedData.append(data, sizeof(uint16_t));

    if (dataLen > compressThreshold &&
        MessageCompressor::compressData(
            compressor, data + sizeof(uint16_t), dataLen - sizeof(uint16_t), compressedData) &&
        compressedData.size() + sizeof(uint16_t) < dataLen) {
        return true;
    } else {
        compressedData.clear();
        return false;
    }
}

bool MessageCompressor::decompressMergedMessage(autil::ZlibCompressor *compressor,
                                                const char *data,
                                                size_t len,
                                                string &uncompressData) {
    if (len <= sizeof(uint16_t)) {
        return false;
    }
    uncompressData.append(data, sizeof(uint16_t));
    return MessageCompressor::uncompressData(
        compressor, data + sizeof(uint16_t), len - sizeof(uint16_t), uncompressData);
}

ErrorCode MessageCompressor::decompressMessageInfo(common::MessageInfo &msg, float &ratio) {
    ThreadBasedObjectPool<ZlibCompressor> *compressorPool =
        Singleton<ThreadBasedObjectPool<ZlibCompressor>, ZlibCompressorInstantiation>::getInstance();
    ZlibCompressor *compressor = compressorPool->getObject();
    int64_t compressSize = 0;
    int64_t decompressSize = 0;
    string uncompressData;
    compressor->reset();
    if (msg.isMerged) {
        if (MessageCompressor::decompressMergedMessage(compressor, msg.data.c_str(), msg.data.size(), uncompressData)) {
            msg.data.swap(uncompressData);
            msg.compress = false;
        }
    } else {
        if (MessageCompressor::uncompressData(compressor, msg.data, uncompressData)) {
            msg.data.swap(uncompressData);
            msg.compress = false;
        }
    }
    decompressSize += msg.data.size();
    ratio = decompressSize > 0 ? (float)compressSize / decompressSize : 1.0;
    return ERROR_NONE;
}

bool MessageCompressor::uncompressData(const string &inputData, std::string &uncompressData) {
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    return MessageCompressor::uncompressData(compressor, inputData, uncompressData);
}

} // namespace protocol
} // namespace swift
