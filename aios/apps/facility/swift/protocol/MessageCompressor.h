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
#pragma once

#include <flatbuffers/flatbuffers.h>
#include <functional>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <zlib.h>

#include "autil/Log.h"
#include "autil/Singleton.h"
#include "autil/ZlibCompressor.h"
#include "swift/common/Common.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"

namespace swift {
namespace common {
class MessageInfo;
} // namespace common

namespace protocol {

class ZlibCompressorInstantiation {
protected:
    template <typename T>
    static void create(T *&ptr) {
        T *tmp = new T(std::bind(&ZlibCompressorInstantiation::createZlibCompressor));
        __asm__ __volatile__("" ::: "memory");
        ptr = tmp;
        static std::shared_ptr<T> destroyer(ptr);
    }
    static autil::ZlibCompressor *createZlibCompressor() { return new autil::ZlibCompressor(Z_BEST_SPEED); }
};

class MessageCompressor {
public:
    MessageCompressor();
    ~MessageCompressor();

private:
    MessageCompressor(const MessageCompressor &);
    MessageCompressor &operator=(const MessageCompressor &);

public:
    static void compressProductionMessage(ProductionRequest *request, uint64_t compressThreshold);
    static void compressProductionMessage(ProductionRequest *request, uint64_t compressThreshold, float &ratio);
    static ErrorCode decompressProductionMessage(ProductionRequest *request);
    static ErrorCode decompressProductionMessage(ProductionRequest *request, float &ratio);

    static void compressResponseMessage(MessageResponse *response);
    static void compressResponseMessage(MessageResponse *response, float &ratio);
    static ErrorCode decompressResponseMessage(MessageResponse *response, bool decompMerge = true);
    static ErrorCode decompressResponseMessage(MessageResponse *response, float &ratio, bool decompMerge = true);

    static void compressMessageResponse(MessageResponse *response);
    static void compressMessageResponse(MessageResponse *response, float &ratio);
    static ErrorCode decompressMessageResponse(MessageResponse *response);
    static ErrorCode decompressMessageResponse(MessageResponse *response, float &ratio);

    static void compressProductionRequest(ProductionRequest *request);
    static void compressProductionRequest(ProductionRequest *request, float &ratio);
    static ErrorCode decompressProductionRequest(ProductionRequest *request);
    static ErrorCode decompressProductionRequest(ProductionRequest *request, float &ratio);

    static ErrorCode decompressMessageInfo(common::MessageInfo &msg, float &ratio);

    static bool
    compressData(autil::ZlibCompressor *compressor, const std::string &inputData, std::string &compressData);
    static bool
    compressData(autil::ZlibCompressor *compressor, const char *inputData, size_t dataLen, std::string &compressData);
    static bool
    uncompressData(autil::ZlibCompressor *compressor, const std::string &inputData, std::string &uncompressData);
    static bool
    uncompressData(autil::ZlibCompressor *compressor, const char *inputData, size_t len, std::string &uncompressData);
    static bool uncompressData(const std::string &inputData, std::string &uncompressData);

    static bool compressMergedMessage(autil::ZlibCompressor *compressor,
                                      uint64_t compressThreshold,
                                      const char *data,
                                      size_t dataLen,
                                      std::string &compressData);
    static bool decompressMergedMessage(autil::ZlibCompressor *compressor,
                                        const char *data,
                                        size_t len,
                                        std::string &uncompressData);

private:
    template <class T>
    static void compressMessage(T *type, uint64_t compressThreshold, float &ratio);
    template <class T>
    static ErrorCode decompressMessage(T *type, float &ratio, bool decompressMessage);
    template <class T>
    static void compress(T *type, float &ratio);
    template <class T>
    static ErrorCode decompress(T *type, float &ratio);

    template <class T>
    static void compressPBMessage(T *type, uint64_t compressThreshold, float &ratio);
    template <class T>
    static void compressFBMessage(T *type, uint64_t compressThreshold, float &ratio);

    template <class T>
    static ErrorCode decompressPBMessage(T *type, float &ratio, bool decomMerge);
    template <class T>
    static ErrorCode decompressFBMessage(T *type, float &ratio, bool decomMerge);

private:
    AUTIL_LOG_DECLARE();
};

template <class T>
void MessageCompressor::compressMessage(T *type, uint64_t compressThreshold, float &ratio) {
    ratio = 1;
    if (type->messageformat() == MF_FB) {
        compressFBMessage(type, compressThreshold, ratio);
    } else if (type->messageformat() == MF_PB) {
        compressPBMessage(type, compressThreshold, ratio);
    }
}

template <class T>
void MessageCompressor::compressPBMessage(T *type, uint64_t compressThreshold, float &ratio) {
    auto compressorPool = autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                                           ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    int64_t compressSize = 0;
    int64_t decompressSize = 0;
    uint32_t msgLen = 0;
    for (int i = 0; i < type->msgs_size(); i++) {
        Message *msg = type->mutable_msgs(i);
        msgLen = msg->data().size();
        decompressSize += msgLen;
        if (msgLen > compressThreshold && !msg->compress() &&
            !msg->merged()) { // merged message compress in writebuffer
            compressor->reset();
            compressor->addDataToBufferIn(msg->data());
            if (compressor->compress() && compressor->getBufferOutLen() < msgLen) {
                msg->set_data(compressor->getBufferOut(), compressor->getBufferOutLen());
                msg->set_compress(true);
            }
        }
        compressSize += msg->data().size();
    }
    ratio = decompressSize > 0 ? (float)compressSize / decompressSize : 1.0;
}

template <class T>
void MessageCompressor::compressFBMessage(T *type, uint64_t compressThreshold, float &ratio) {
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();

    int64_t compressSize = 0;
    int64_t decompressSize = 0;
    uint32_t msgLen = 0;
    if (!type->has_fbmsgs() || type->fbmsgs().size() == 0) {
        return;
    }
    FBMessageReader reader;
    if (!reader.init(type->fbmsgs(), false)) {
        return;
    }
    uint32_t msgSize = reader.size();
    bool needRebuild = false;
    for (uint32_t i = 0; i < msgSize; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        msgLen = fbMsg->data()->size();
        if (msgLen > compressThreshold && !fbMsg->compress() && !fbMsg->merged()) {
            needRebuild = true;
            break;
        }
    }
    if (!needRebuild) {
        return;
    }
    swift::common::ThreadBasedObjectPool<FBMessageWriter> *objectPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
    FBMessageWriter *writer = objectPool->getObject();
    writer->reset();
    for (uint32_t i = 0; i < msgSize; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        msgLen = fbMsg->data()->size();
        decompressSize += msgLen;
        bool isCompressed = fbMsg->compress();
        if (msgLen > compressThreshold && !isCompressed && !fbMsg->merged()) {
            compressor->reset();
            compressor->addDataToBufferIn(fbMsg->data()->c_str(), fbMsg->data()->size());
            if (compressor->compress() && compressor->getBufferOutLen() < msgLen) {
                msgLen = compressor->getBufferOutLen();
                isCompressed = true;
                writer->addMessage(compressor->getBufferOut(),
                                   compressor->getBufferOutLen(),
                                   fbMsg->msgId(),
                                   fbMsg->timestamp(),
                                   fbMsg->uint16Payload(),
                                   fbMsg->uint8MaskPayload(),
                                   isCompressed,
                                   fbMsg->merged());
            } else {
                writer->addMessage(fbMsg->data()->c_str(),
                                   fbMsg->data()->size(),
                                   fbMsg->msgId(),
                                   fbMsg->timestamp(),
                                   fbMsg->uint16Payload(),
                                   fbMsg->uint8MaskPayload(),
                                   isCompressed,
                                   fbMsg->merged());
            }
        } else {
            writer->addMessage(fbMsg->data()->c_str(),
                               fbMsg->data()->size(),
                               fbMsg->msgId(),
                               fbMsg->timestamp(),
                               fbMsg->uint16Payload(),
                               fbMsg->uint8MaskPayload(),
                               isCompressed,
                               fbMsg->merged());
        }
        compressSize += msgLen;
    }
    type->set_fbmsgs(writer->data(), writer->size());
    ratio = decompressSize > 0 ? (float)compressSize / decompressSize : 1.0;
}

template <class T>
ErrorCode MessageCompressor::decompressMessage(T *type, float &ratio, bool decompressMerge) {
    ratio = 1.0;
    if (type->messageformat() == MF_FB) {
        return decompressFBMessage(type, ratio, decompressMerge);
    } else if (type->messageformat() == MF_PB) {
        return decompressPBMessage(type, ratio, decompressMerge);
    }
    return ERROR_DECOMPRESS_MESSAGE;
}

template <class T>
ErrorCode MessageCompressor::decompressPBMessage(T *type, float &ratio, bool decomMerge) {
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    int64_t compressSize = 0;
    int64_t decompressSize = 0;
    std::string uncompressData;
    for (int i = 0; i < type->msgs_size(); i++) {
        Message *msg = type->mutable_msgs(i);
        compressSize += msg->data().size();
        if (msg->compress()) {
            compressor->reset();
            if (msg->merged()) {
                if (decomMerge) {
                    uncompressData.clear();
                    if (MessageCompressor::decompressMergedMessage(
                            compressor, msg->data().c_str(), msg->data().size(), uncompressData)) {
                        msg->mutable_data()->swap(uncompressData);
                        msg->set_compress(false);
                    }
                }
            } else {
                compressor->addDataToBufferIn(msg->data());
                if (compressor->decompress()) {
                    msg->set_data(compressor->getBufferOut(), compressor->getBufferOutLen());
                    msg->set_compress(false);
                }
            }
        }
        decompressSize += msg->data().size();
    }
    ratio = decompressSize > 0 ? (float)compressSize / decompressSize : 1.0;
    return ERROR_NONE;
}

template <class T>
ErrorCode MessageCompressor::decompressFBMessage(T *type, float &ratio, bool decomMerge) {
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    int64_t compressSize = 0;
    int64_t decompressSize = 0;
    if (!type->has_fbmsgs() || type->fbmsgs().size() == 0) {
        return ERROR_DECOMPRESS_MESSAGE;
    }
    FBMessageReader reader;
    if (!reader.init(type->fbmsgs(), false)) {
        return ERROR_DECOMPRESS_MESSAGE;
    }
    uint32_t msgSize = reader.size();
    bool needRebuild = false;
    for (uint32_t i = 0; i < msgSize; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        if (fbMsg->compress()) {
            if (fbMsg->merged()) {
                if (decomMerge) {
                    needRebuild = true;
                    break;
                }
            } else {
                needRebuild = true;
                break;
            }
        }
    }
    if (!needRebuild) {
        return ERROR_NONE;
    }
    swift::common::ThreadBasedObjectPool<FBMessageWriter> *objectPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
    FBMessageWriter *writer = objectPool->getObject();
    writer->reset();
    uint32_t msgLen = 0;
    std::string uncompressData;
    for (uint32_t i = 0; i < msgSize; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        msgLen = fbMsg->data()->size();
        compressSize += msgLen;
        bool isCompressed = fbMsg->compress();
        if (isCompressed) {
            compressor->reset();
            if (fbMsg->merged()) {
                uncompressData.clear();
                if (decomMerge && MessageCompressor::decompressMergedMessage(
                                      compressor, fbMsg->data()->c_str(), fbMsg->data()->size(), uncompressData)) {
                    isCompressed = false;
                    writer->addMessage(uncompressData.c_str(),
                                       uncompressData.size(),
                                       fbMsg->msgId(),
                                       fbMsg->timestamp(),
                                       fbMsg->uint16Payload(),
                                       fbMsg->uint8MaskPayload(),
                                       isCompressed,
                                       fbMsg->merged());
                    msgLen = uncompressData.size();
                } else {
                    writer->addMessage(fbMsg->data()->c_str(),
                                       fbMsg->data()->size(),
                                       fbMsg->msgId(),
                                       fbMsg->timestamp(),
                                       fbMsg->uint16Payload(),
                                       fbMsg->uint8MaskPayload(),
                                       isCompressed,
                                       fbMsg->merged());
                }
            } else {
                compressor->addDataToBufferIn(fbMsg->data()->c_str(), fbMsg->data()->size());
                if (compressor->decompress()) {
                    isCompressed = false;
                    writer->addMessage(compressor->getBufferOut(),
                                       compressor->getBufferOutLen(),
                                       fbMsg->msgId(),
                                       fbMsg->timestamp(),
                                       fbMsg->uint16Payload(),
                                       fbMsg->uint8MaskPayload(),
                                       isCompressed,
                                       fbMsg->merged());
                    msgLen = compressor->getBufferOutLen();
                } else {
                    writer->addMessage(fbMsg->data()->c_str(),
                                       fbMsg->data()->size(),
                                       fbMsg->msgId(),
                                       fbMsg->timestamp(),
                                       fbMsg->uint16Payload(),
                                       fbMsg->uint8MaskPayload(),
                                       isCompressed,
                                       fbMsg->merged());
                }
            }
        } else {
            writer->addMessage(fbMsg->data()->c_str(),
                               fbMsg->data()->size(),
                               fbMsg->msgId(),
                               fbMsg->timestamp(),
                               fbMsg->uint16Payload(),
                               fbMsg->uint8MaskPayload(),
                               isCompressed,
                               fbMsg->merged());
        }
        decompressSize += msgLen;
    }
    writer->finish();
    type->set_fbmsgs(writer->data(), writer->size());
    ratio = decompressSize > 0 ? (float)compressSize / decompressSize : 1.0;
    return ERROR_NONE;
}

template <class T>
void MessageCompressor::compress(T *type, float &ratio) {
    ratio = 1.0;
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    compressor->reset();
    uint32_t length = 0;
    if (type->messageformat() == MF_FB) {
        if (!type->has_fbmsgs() || type->fbmsgs().size() == 0) {
            return;
        }
        const std::string &data = type->fbmsgs();
        length = data.length();
        compressor->addDataToBufferIn(data);
        if (!compressor->compress()) {
            return;
        }
        type->clear_fbmsgs();
    } else if (type->messageformat() == MF_PB) {
        if (type->msgs_size() == 0) {
            return;
        }
        Messages tmpMessage;
        ::google::protobuf::RepeatedPtrField<Message> *tmpMsgs = tmpMessage.mutable_msgs();
        ::google::protobuf::RepeatedPtrField<Message> *msgs = type->mutable_msgs();
        tmpMsgs->Swap(msgs);
        std::string messageStr;
        tmpMessage.SerializeToString(&messageStr);
        compressor->addDataToBufferIn(messageStr);
        if (!compressor->compress()) {
            msgs->Swap(tmpMsgs);
            return;
        }
        length = messageStr.length();
    } else {
        return;
    }

    std::string *compressedStr = type->mutable_compressedmsgs();
    compressedStr->assign(compressor->getBufferOut(), compressor->getBufferOutLen());

    if (length > 0) {
        ratio = (float)compressedStr->size() / length;
    }
}

template <class T>
ErrorCode MessageCompressor::decompress(T *type, float &ratio) {
    ratio = 1.0;
    if (!type->has_compressedmsgs()) {
        return ERROR_DECOMPRESS_MESSAGE;
    }

    const std::string &compressedMsgs = type->compressedmsgs();
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    compressor->reset();
    compressor->addDataToBufferIn(compressedMsgs);
    if (!compressor->decompress()) {
        return ERROR_DECOMPRESS_MESSAGE;
    }
    uint32_t rawLength = compressor->getBufferOutLen();
    if (type->messageformat() == MF_FB) {
        type->set_fbmsgs(compressor->getBufferOut(), rawLength);
    } else if (type->messageformat() == MF_PB) {
        Messages tmpMessages;
        if (!tmpMessages.ParseFromArray(compressor->getBufferOut(), rawLength)) {
            return ERROR_DECOMPRESS_MESSAGE;
        }
        type->mutable_msgs()->Swap(tmpMessages.mutable_msgs());
    } else {
        return ERROR_DECOMPRESS_MESSAGE;
    }

    if (rawLength > 0) {
        ratio = (float)compressedMsgs.size() / rawLength;
    }
    return ERROR_NONE;
}

SWIFT_TYPEDEF_PTR(MessageCompressor);

} // namespace protocol
} // namespace swift
