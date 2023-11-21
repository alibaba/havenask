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
#include "swift/client/MessageReadBuffer.h"

#include <cstddef>
#include <flatbuffers/flatbuffers.h>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/client/DecompressMessageWorkItem.h"
#include "swift/client/Notifier.h"
#include "swift/client/trace/SwiftErrorResponseCollector.h"
#include "swift/client/trace/SwiftFatalErrorCollector.h"
#include "swift/filter/FieldFilter.h"
#include "swift/filter/MessageFilter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/protocol/TraceMessage.pb.h"
#include "swift/util/MessageUtil.h"

using namespace std;
using namespace swift::util;
using namespace swift::protocol;
using namespace swift::common;
using namespace std;
using namespace autil;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, MessageReadBuffer);
AUTIL_LOG_SETUP(swift, SingleResponse);

SingleResponse::SingleResponse(const protocol::MessageResponse *response) {
    _response = response;
    _readPos = 0;
    _msgFormat = MF_PB;
    _msgCount = 0;
    _totalMsgCount = 0;
    _requestUuid = 0;
    _needDecompress = false;
    _notifier = new Notifier();
}

SingleResponse::~SingleResponse() {
    AUTIL_LOG(DEBUG,
              "[%s %u] delete a response. total msg count [%ld] in [%ld]",
              _readerInfo.topicname().c_str(),
              _readerInfo.partitionid(),
              _totalMsgCount,
              _msgCount);
    if (_readPos < _msgCount) {
        AUTIL_LOG(INFO,
                  "[%s %u] single response still has [%ld] not read.",
                  _readerInfo.topicname().c_str(),
                  _readerInfo.partitionid(),
                  _msgCount - _readPos);
    }
    if (_notifier) {
        _notifier->wait(-1);
        DELETE_AND_SET_NULL(_notifier);
    }
    DELETE_AND_SET_NULL(_response);
}

bool SingleResponse::init() {
    if (_response == nullptr) {
        AUTIL_LOG(WARN, "response is empty!");
        return false;
    }
    _readPos = 0;
    _msgFormat = _response->messageformat();
    if (_msgFormat == MF_FB) {
        if (!_reader.init(_response->fbmsgs(), true)) {
            AUTIL_LOG(ERROR,
                      "[%s %u %ld] fb message init failed.",
                      _readerInfo.topicname().c_str(),
                      _readerInfo.partitionid(),
                      _response->requestuuid());
            return false;
        }
        _msgCount = _reader.size();
        for (uint32_t i = 0; i < _msgCount; i++) {
            if (_reader.read(i)->compress()) {
                _needDecompress = true;
                break;
            }
        }
    } else {
        _msgCount = _response->msgs_size();
        for (int i = 0; i < _response->msgs_size(); i++) {
            if (_response->msgs(i).compress()) {
                _needDecompress = true;
                break;
            }
        }
    }
    if (_response->has_totalmsgcount()) {
        _totalMsgCount = _response->totalmsgcount();
    } else {
        _totalMsgCount = _msgCount;
    }
    _requestUuid = _response->requestuuid();
    return true;
}

void SingleResponse::setReaderInfo(const protocol::ReaderInfo &readerInfo) { _readerInfo = readerInfo; }

bool SingleResponse::read(protocol::Message &msg) {
    if (_readPos < _msgCount) {
        if (_msgFormat == MF_FB) {
            const protocol::flat::Message *fbMsg = _reader.read(_readPos++);
            if (fbMsg) {
                convertFB2PBMsg(*fbMsg, msg);
            } // NULL fbMsg is illegal msg, use default value
            msg.set_requestuuid(_requestUuid);
            return true;
        } else if (_msgFormat == MF_PB) {
            msg = _response->msgs(_readPos++);
            msg.set_requestuuid(_requestUuid);
            return true;
        }
    }
    return false;
}

int64_t SingleResponse::getFirstMsgTimestamp() const {
    if (_readPos < _msgCount) {
        if (_msgFormat == MF_FB) {
            const protocol::flat::Message *fbMsg = _reader.read(_readPos);
            if (fbMsg) {
                return fbMsg->timestamp();
            } else {
                return 0; // NULL fbMsg is illegal msg, use default value
            }
        } else if (_msgFormat == MF_PB) {
            return _response->msgs(_readPos).timestamp();
        }
    }
    return -1;
}

void SingleResponse::convertFB2PBMsg(const protocol::flat::Message &fbMsg, protocol::Message &msg) {
    msg.set_msgid(fbMsg.msgId());
    msg.set_uint16payload(fbMsg.uint16Payload());
    msg.set_uint8maskpayload(fbMsg.uint8MaskPayload());
    msg.set_timestamp(fbMsg.timestamp());
    msg.set_compress(fbMsg.compress());
    msg.set_merged(fbMsg.merged());
    msg.set_data(fbMsg.data()->Data(), fbMsg.data()->size());
}

bool SingleResponse::waitDecompressDone() {
    if (!_needDecompress) {
        return true;
    }
    if (!_notifier->wait(-1)) {
        AUTIL_LOG(ERROR, "wait notifier failed.");
        return false;
    }
    if (_response->errorinfo().errcode() == ERROR_DECOMPRESS_MESSAGE) {
        string errInfo = "[" + _readerInfo.topicname() + " " + StringUtil::toString(_readerInfo.partitionid()) + " " +
                         StringUtil::toString(_response->requestuuid()) +
                         "] decompress msg has error: " + _response->errorinfo().ShortDebugString();
        AUTIL_LOG(ERROR, "%s", errInfo.c_str());
        auto *errCollector = ErrorCollectorSingleton::getInstance();
        errCollector->addRequestHasErrorDataInfo(errInfo);
        auto *responseCollector = ResponseCollectorSingleton::getInstance();
        string content;
        if (_response->SerializeToString(&content)) {
            responseCollector->logResponse(_readerInfo, MESSAGE_RESPONSE, content);
        } else {
            responseCollector->logResponse(_readerInfo, MESSAGE_RESPONSE_FBMSG_PART, _response->fbmsgs());
        }
        // return false; // return raw compress message, other skip message
    }
    if (_msgFormat == MF_FB) {
        if (!_reader.init(_response->fbmsgs(), true)) {
            AUTIL_LOG(ERROR,
                      "[%s %u %ld] decompressed fb message init failed.",
                      _readerInfo.topicname().c_str(),
                      _readerInfo.partitionid(),
                      _response->requestuuid());
            return false;
        }
        if (_msgCount != (int64_t)_reader.size()) {
            AUTIL_LOG(ERROR,
                      "[%s %u %ld] decompress data reader msg count not equal [%ld %lu].",
                      _readerInfo.topicname().c_str(),
                      _readerInfo.partitionid(),
                      _response->requestuuid(),
                      _msgCount,
                      _reader.size());
            return false;
        }
    }
    return true;
}

MessageReadBuffer::MessageReadBuffer()
    : _frontResponse(NULL), _readOffset(0), _metaFilter(NULL), _fieldFilter(NULL), _partitionId(0) {}

MessageReadBuffer::~MessageReadBuffer() {
    if (_unReadMsgCount.get() > 0) {
        AUTIL_LOG(
            INFO, "[%s %d] read buffer still has [%d] msg.", _topicName.c_str(), _partitionId, _unReadMsgCount.get());
    } else {
        AUTIL_LOG(INFO, "[%s %d] all msg in buffer has read.", _topicName.c_str(), _partitionId);
    }
    clear();
    DELETE_AND_SET_NULL(_metaFilter);
    DELETE_AND_SET_NULL(_fieldFilter);
}

bool MessageReadBuffer::addResponse(protocol::MessageResponse *response) {
    if (response == NULL) {
        return false;
    }
    SingleResponse *single = new SingleResponse(response);
    single->setReaderInfo(_readerInfo);
    if (!single->init()) {
        DELETE_AND_SET_NULL(single);
        AUTIL_LOG(ERROR, "[%s %u] response init failed.", _topicName.c_str(), _partitionId);
        return false;
    }
    if (single->getTotalMsgCount() > 0) {
        if (single->needDecompress()) {
            Notifier *notifier = single->getNotifier();
            notifier->setNeedNotify();
            auto *workItem = new DecompressMessageWorkItem(response, notifier);
            if (_decompressThreadPool) {
                auto ec = _decompressThreadPool->pushWorkItem(workItem);
                if (ec != autil::ThreadPool::ERROR_NONE) {
                    AUTIL_LOG(ERROR, "[%s %u] add decompress queue failed.", _topicName.c_str(), _partitionId);
                    workItem->process();
                    DELETE_AND_SET_NULL(workItem);
                }
            } else {
                AUTIL_LOG(INFO, "[%s %u] decompress in handle thread.", _topicName.c_str(), _partitionId);
                workItem->process();
                DELETE_AND_SET_NULL(workItem);
            }
        }
        AUTIL_LOG(DEBUG,
                  "[%s %u] add a response, msg count [%ld], total msg [%d].",
                  _topicName.c_str(),
                  _partitionId,
                  single->getTotalMsgCount(),
                  _unReadMsgCount.get());
        autil::ScopedLock lock(_mutex);
        _responseList.push_back(single);
        _unReadMsgCount.add(single->getTotalMsgCount());
    } else {
        DELETE_AND_SET_NULL(single);
    }
    return true;
}

bool MessageReadBuffer::read(protocol::Message &msg) {
    if (_readOffset < _unpackMsgVec.size()) {
        msg.Swap(&_unpackMsgVec[_readOffset++]);
        _unReadMsgCount.decrement();
        return true;
    }
    if (!fillMessage()) {
        return false;
    }
    if (_readOffset < _unpackMsgVec.size()) {
        msg.Swap(&_unpackMsgVec[_readOffset++]);
        _unReadMsgCount.decrement();
        return true;
    }
    return false;
}

int64_t MessageReadBuffer::getFirstMsgTimestamp() {
    if (_readOffset < _unpackMsgVec.size()) {
        return _unpackMsgVec[_readOffset].timestamp();
    }
    if (!fillMessage()) {
        return -1;
    }
    if (_readOffset < _unpackMsgVec.size()) {
        return _unpackMsgVec[_readOffset].timestamp();
    }
    return -1;
}

bool MessageReadBuffer::fillMessage() {
    if (_readOffset < _unpackMsgVec.size()) {
        return true;
    }
    if (_unReadMsgCount.get() <= 0) {
        return false;
    }
    do {
        protocol::Message msg;
        if (!doRead(msg)) {
            return false;
        }
        if (!msg.merged()) {
            _readOffset = 0;
            _unpackMsgVec.clear();
            _unpackMsgVec.emplace_back(msg);
            return true;
        } else {
            int32_t totalCount = 0;
            if (unpackMessage(msg, totalCount)) {
                _unReadMsgCount.add(int32_t(_unpackMsgVec.size()) - totalCount);
            }
        }
    } while (_unpackMsgVec.empty());
    return true;
}

bool MessageReadBuffer::doRead(protocol::Message &msg) {
    if (_frontResponse != NULL && _frontResponse->read(msg)) {
        return true;
    }
    if (_frontResponse != NULL) {
        DELETE_AND_SET_NULL(_frontResponse);
    }
    _frontResponse = getNextResponse();
    if (_frontResponse == NULL) {
        AUTIL_LOG(ERROR,
                  "[%s %u] response list size is [%lu], unread msg count [%d]",
                  _topicName.c_str(),
                  _partitionId,
                  _responseList.size(),
                  _unReadMsgCount.get());
        autil::ScopedLock lock(_mutex);
        if (_responseList.size() == 0) {
            AUTIL_LOG(ERROR, "[%s %d] reset unread msg count.", _topicName.c_str(), _partitionId);
            _unReadMsgCount.getAndSet(0);
        }
        return false;
    }
    return _frontResponse->read(msg);
}

bool MessageReadBuffer::unpackMessage(const protocol::Message &msg, int32_t &totalCount) {
    _readOffset = 0;
    _unpackMsgVec.clear();
    bool ret = MessageUtil::unpackMessage(NULL,
                                          msg.data().c_str(),
                                          msg.data().size(),
                                          msg.msgid(),
                                          msg.timestamp(),
                                          _metaFilter,
                                          _fieldFilter,
                                          _unpackMsgVec,
                                          totalCount);
    if (ret) {
        for (auto &unpackMsg : _unpackMsgVec) {
            unpackMsg.set_requestuuid(msg.requestuuid());
        }
        AUTIL_LOG(DEBUG,
                  "[%s %d] unpack [%lu] message , total [%d]",
                  _topicName.c_str(),
                  _partitionId,
                  _unpackMsgVec.size(),
                  totalCount);
        return true;
    } else {
        AUTIL_LOG(WARN, "[%s %d] unpack message id [%ld] failed", _topicName.c_str(), _partitionId, msg.msgid());
        return false;
    }
}

void MessageReadBuffer::clear() {
    autil::ScopedLock lock(_mutex);
    DELETE_AND_SET_NULL(_frontResponse);
    while (!_responseList.empty()) {
        SingleResponse *response = _responseList.front();
        DELETE_AND_SET_NULL(response);
        _responseList.pop_front();
    }
    _unReadMsgCount.getAndSet(0);
    _unpackMsgVec.clear();
    _readOffset = 0;
}

int64_t MessageReadBuffer::getUnReadMsgCount() { return _unReadMsgCount.get(); }

int64_t MessageReadBuffer::seek(int64_t msgid) {
    clear();
    return msgid;
}

SingleResponse *MessageReadBuffer::getNextResponse() {
    autil::ScopedLock lock(_mutex);
    if (_responseList.size() == 0) {
        return NULL;
    }
    SingleResponse *single = NULL;
    while (!_responseList.empty()) {
        single = _responseList.front();
        _responseList.pop_front();
        if (single->getMsgCount() <= 0) {
            DELETE_AND_SET_NULL(single);
        }
        if (single != NULL) {
            if (!single->waitDecompressDone()) {
                DELETE_AND_SET_NULL(single);
                AUTIL_LOG(ERROR, "[%s %u] response docompress failed.", _topicName.c_str(), _partitionId);
            } else {
                break;
            }
        }
    }
    return single;
}

void MessageReadBuffer::updateFilter(const Filter &filter,
                                     const std::vector<std::string> &requiredFileds,
                                     const std::string &filterDesc) {
    DELETE_AND_SET_NULL(_metaFilter);
    DELETE_AND_SET_NULL(_fieldFilter);
    _metaFilter = new swift::filter::MessageFilter(filter);
    if (requiredFileds.size() != 0 || !filterDesc.empty()) {
        _fieldFilter = new swift::filter::FieldFilter();
        if (ERROR_NONE != _fieldFilter->init(requiredFileds, filterDesc)) {
            AUTIL_LOG(
                WARN, "[%s %u] updata filter faield, desc [%s].", _topicName.c_str(), _partitionId, filterDesc.c_str());
            DELETE_AND_SET_NULL(_fieldFilter);
        }
    }
}

void MessageReadBuffer::setTopicName(const std::string &topicName) { _topicName = topicName; }

void MessageReadBuffer::setPartitionId(uint32_t partitionId) { _partitionId = partitionId; }

void MessageReadBuffer::setDecompressThreadPool(autil::ThreadPoolPtr decompressThreadPool) {
    _decompressThreadPool = decompressThreadPool;
}
void MessageReadBuffer::setReaderInfo(const protocol::ReaderInfo &readerInfo) { _readerInfo = readerInfo; }

} // namespace client
} // namespace swift
