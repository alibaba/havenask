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
#include "swift/filter/FieldFilter.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/Singleton.h"
#include "autil/Span.h"
#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/stl_emulation.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/filter/DescFieldFilter.h"
#include "swift/filter/EliminateFieldFilter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"

using namespace swift::protocol;
using namespace swift::common;
namespace swift {
namespace filter {
using namespace flatbuffers;
using namespace std;
AUTIL_LOG_SETUP(swift, FieldFilter);

FieldFilter::FieldFilter() {
    _elimFilter = NULL;
    _descFilter = NULL;
}

FieldFilter::~FieldFilter() {
    DELETE_AND_SET_NULL(_elimFilter);
    DELETE_AND_SET_NULL(_descFilter);
}

ErrorCode FieldFilter::init(const ConsumptionRequest *request) {
    if (request->requiredfieldnames_size() != 0) {
        _elimFilter = new EliminateFieldFilter();
        for (size_t i = 0; i < (size_t)request->requiredfieldnames_size(); ++i) {
            const string &requiredName = request->requiredfieldnames(i);
            autil::StringView fieldName(requiredName.c_str(), requiredName.size());
            _elimFilter->addRequiredField(fieldName, i);
        }
    }
    if (request->has_fieldfilterdesc() && request->fieldfilterdesc().size() > 0) {
        _descFilter = new DescFieldFilter();
        const string &filterDesc = request->fieldfilterdesc();
        if (!_descFilter->init(filterDesc)) {
            AUTIL_LOG(WARN,
                      "[%s %d] init field filter desc failed. desc[%s]",
                      _topicName.c_str(),
                      _partId,
                      filterDesc.c_str());
            return ERROR_BROKER_INIT_FIELD_FILTER_FAILD;
        }
    }
    return ERROR_NONE;
}

ErrorCode FieldFilter::init(const vector<string> &requiredFileds, const string &filterDesc) {
    if (requiredFileds.size() != 0) {
        _elimFilter = new EliminateFieldFilter();
        for (size_t i = 0; i < (size_t)requiredFileds.size(); ++i) {
            const string &requiredName = requiredFileds[i];
            autil::StringView fieldName(requiredName.c_str(), requiredName.size());
            _elimFilter->addRequiredField(fieldName, i);
        }
    }
    if (filterDesc.size() > 0) {
        _descFilter = new DescFieldFilter();
        if (!_descFilter->init(filterDesc)) {
            AUTIL_LOG(WARN,
                      "[%s %d] init field filter desc failed. desc[%s]",
                      _topicName.c_str(),
                      _partId,
                      filterDesc.c_str());
            return ERROR_BROKER_INIT_FIELD_FILTER_FAILD;
        }
    }
    return ERROR_NONE;
}

void FieldFilter::filter(protocol::MessageResponse *response,
                         int64_t &totalSize,
                         int64_t &rawTotalSize,
                         int64_t &readMsgCount,
                         int64_t &updateMsgCount) {
    if (response->messageformat() == MF_PB) {
        filterPBMessage(response, totalSize, rawTotalSize, readMsgCount, updateMsgCount);
    } else if (response->messageformat() == MF_FB) {
        filterFBMessage(response, totalSize, rawTotalSize, readMsgCount, updateMsgCount);
    }
}
void FieldFilter::filterPBMessage(protocol::MessageResponse *response,
                                  int64_t &totalSize,
                                  int64_t &rawTotalSize,
                                  int64_t &readMsgCount,
                                  int64_t &updateMsgCount) {
    readMsgCount += response->msgs_size();
    MessageResponse tmpResponse;
    google::protobuf::RepeatedPtrField<protocol::Message> *tmpMsgs = tmpResponse.mutable_msgs();
    google::protobuf::RepeatedPtrField<protocol::Message> *msgs = response->mutable_msgs();
    tmpMsgs->Swap(msgs);
    int64_t filterCount = 0;
    for (int i = 0; i < tmpMsgs->size(); ++i) {
        Message *msg = tmpMsgs->Mutable(i);
        int64_t rawMsgLen = msg->data().length();
        rawTotalSize += rawMsgLen;
        string msgData;
        if (msg->merged()) {
            Message *addMsg = response->add_msgs();
            addMsg->Swap(msg);
            totalSize += addMsg->data().size();
            updateMsgCount++;
        } else if (filter(msg->data(), msgData)) {
            Message *addMsg = response->add_msgs();
            addMsg->Swap(msg);
            addMsg->set_data(msgData);
            size_t msgDataLen = msgData.length();
            totalSize += msgDataLen;
            updateMsgCount++;
        } else {
            filterCount++;
        }
    }
    if (response->has_totalmsgcount()) {
        response->set_totalmsgcount(response->totalmsgcount() - filterCount);
    }
}
void FieldFilter::filterFBMessage(protocol::MessageResponse *response,
                                  int64_t &totalSize,
                                  int64_t &rawTotalSize,
                                  int64_t &readMsgCount,
                                  int64_t &updateMsgCount) {
    FBMessageReader reader;
    reader.init(response->fbmsgs());
    uint32_t msgCount = reader.size();
    readMsgCount += msgCount;
    ThreadBasedObjectPool<FBMessageWriter> *objectPool =
        autil::Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
    FBMessageWriter *writer = objectPool->getObject();
    writer->reset();
    uint32_t msgLen = 0;
    string output;
    int64_t filterCount = 0;
    for (uint32_t i = 0; i < msgCount; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        msgLen = fbMsg->data()->size();
        rawTotalSize += msgLen;
        output.clear();
        if (fbMsg->merged()) {
            writer->addMessage(fbMsg->data()->c_str(),
                               msgLen,
                               fbMsg->msgId(),
                               fbMsg->timestamp(),
                               fbMsg->uint16Payload(),
                               fbMsg->uint8MaskPayload(),
                               fbMsg->compress(),
                               fbMsg->merged());
            totalSize += msgLen;
            updateMsgCount++;
        } else if (filter(fbMsg->data()->c_str(), msgLen, output)) {
            writer->addMessage(output,
                               fbMsg->msgId(),
                               fbMsg->timestamp(),
                               fbMsg->uint16Payload(),
                               fbMsg->uint8MaskPayload(),
                               fbMsg->compress(),
                               fbMsg->merged());
            totalSize += msgLen;
            updateMsgCount++;
        } else {
            filterCount++;
        }
    }
    writer->finish();
    response->set_fbmsgs(writer->data(), writer->size());
    if (response->has_totalmsgcount()) {
        response->set_totalmsgcount(response->totalmsgcount() - filterCount);
    }
}

bool FieldFilter::filter(const std::string &inputMsg, std::string &outputMsg) {
    return filter(inputMsg.c_str(), inputMsg.size(), outputMsg);
}

bool FieldFilter::filter(const char *inputMsg, size_t msgLen, std::string &outputMsg) {
    if (!_fieldGroupReader.fromProductionString(inputMsg, msgLen)) {
        AUTIL_LOG(WARN, "[%s %d] parse field message error.", _topicName.c_str(), _partId);
        return false;
    }
    if (_descFilter != NULL && !_descFilter->filterMsg(_fieldGroupReader)) {
        return false;
    }
    _fieldGroupWriter.reset();
    if (_elimFilter != NULL && !_elimFilter->filterFields(_fieldGroupReader, _fieldGroupWriter)) {
        return false;
    }

    if (!_fieldGroupWriter.empty()) {
        _fieldGroupWriter.toString(outputMsg);
    } else {
        outputMsg.assign(inputMsg, msgLen);
    }

    return true;
}

void FieldFilter::setTopicName(const std::string &topicName) { _topicName = topicName; }
void FieldFilter::setPartId(uint32_t id) { _partId = id; }

} // namespace filter
} // namespace swift
