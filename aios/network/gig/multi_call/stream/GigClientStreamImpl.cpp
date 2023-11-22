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
#include "aios/network/gig/multi_call/stream/GigClientStreamImpl.h"

#include "aios/network/gig/multi_call/grpc/client/GrpcClientStreamHandler.h"
#include "aios/network/gig/multi_call/service/ChildNodeReply.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/stream/GigClientStream.h"
#include "aios/network/gig/multi_call/stream/GigStreamRequest.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigClientStreamImpl);

GigClientStreamImpl::GigClientStreamImpl(GigClientStream *stream)
    : _stream(stream)
    , _partCount(INVALID_PART_COUNT) {
    assert(stream);
}

GigClientStreamImpl::~GigClientStreamImpl() {
    if (_reply) {
        _reply->setNeedEndStream(true);
    }
    if (_caller) {
        _caller->stop();
    }
}

bool GigClientStreamImpl::init(const ChildNodeReplyPtr &reply,
                               const SearchServiceResourceVector &resourceVec,
                               const CallerPtr &caller, bool disableRetry, bool forceStop,
                               const std::set<PartIdTy> &enablePartIds, bool allowLack)
{
    _reply = reply;
    _caller = caller;
    bool hasError = false;
    int32_t enableCount = enablePartIds.size();
    int32_t enableReadyCount = 0;
    int32_t normalCount = 0;
    for (const auto &resource : resourceVec) {
        if (resource->getHasError()) {
            AUTIL_LOG(ERROR, "stream post has error, check stream port or provider post");
            hasError = true;
            break;
        }
        auto request = resource->getRequest();
        auto streamRequest = dynamic_pointer_cast<GigStreamRequest>(request);
        if (!streamRequest) {
            AUTIL_LOG(ERROR, "null stream request");
            hasError = true;
            break;
        }
        streamRequest->setDisableRetry(disableRetry);
        streamRequest->setForceStop(forceStop);
        auto partId = resource->getPartId();
        _requestMap[partId] = streamRequest;
        _partCount = resource->getPartCnt();
        if (resource->isNormalRequest()) {
            if (enableCount > 0) {
                if (enablePartIds.end() == enablePartIds.find(partId)) {
                    AUTIL_INTERVAL_LOG(
                        200, ERROR,
                        "provider partId mismatch, partId [%d] not enabled, partCount[%d]", partId,
                        _partCount);
                    hasError = true;
                    break;
                } else {
                    enableReadyCount++;
                }
            } else {
                normalCount++;
            }
        }
    }
    if (INVALID_PART_COUNT == _partCount) {
        hasError = true;
    }
    if (!hasError && !allowLack) {
        if (enableCount > 0) {
            if (enableCount != enableReadyCount) {
                AUTIL_INTERVAL_LOG(
                    200, ERROR,
                    "lack enabled provider, expect count [%d] actual count [%d], lack not allowed",
                    enableCount, enableReadyCount);
                hasError = true;
            }
        } else if (_partCount != normalCount) {
            AUTIL_INTERVAL_LOG(
                200, ERROR, "lack provider, expect count [%d] actual count [%d], lack not allowed",
                _partCount, normalCount);
            hasError = true;
        }
    }
    if (hasError) {
        abort();
        return false;
    }
    return true;
}

void GigClientStreamImpl::abort() {
    for (const auto &pair : _requestMap) {
        const auto &request = pair.second;
        request->abort();
    }
}

PartIdTy GigClientStreamImpl::getPartCount() const {
    return _partCount;
}

bool GigClientStreamImpl::send(PartIdTy partId, bool eof, google::protobuf::Message *message) {
    if (!message && !eof) {
        AUTIL_LOG(ERROR, "send failed, null msg");
        return false;
    }
    GigStreamMessage streamMessage;
    streamMessage.message = message;
    streamMessage.partId = partId;
    streamMessage.eof = eof;
    return writeMessage(false, streamMessage);
}

bool GigClientStreamImpl::writeMessage(bool cancel, const GigStreamMessage &message) {
    auto partId = message.partId;
    auto it = _requestMap.find(partId);
    if (_requestMap.end() != it) {
        const auto &request = it->second;
        if (!request->post(cancel, message)) {
            return false;
        }
    } else {
        AUTIL_LOG(DEBUG, "write message failed, partId [%d] not found, cancel [%d]", partId,
                  cancel);
        return false;
    }
    return true;
}

void GigClientStreamImpl::sendCancel(PartIdTy partId, google::protobuf::Message *message) {
    GigStreamMessage streamMessage;
    streamMessage.message = message;
    streamMessage.partId = partId;
    streamMessage.eof = true;
    writeMessage(true, streamMessage);
}

GigStreamRpcInfo GigClientStreamImpl::getStreamRpcInfo(PartIdTy partId) const {
    auto it = _requestMap.find(partId);
    if (_requestMap.end() == it) {
        return {partId};
    }
    const auto &request = it->second;
    auto handler = request->getResultHandler();
    if (handler) {
        return handler->snapshotStreamRpcInfo();
    } else {
        return {partId};
    }
}

} // namespace multi_call
