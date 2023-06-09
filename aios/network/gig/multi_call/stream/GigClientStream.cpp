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
#include "aios/network/gig/multi_call/stream/GigClientStream.h"
#include "aios/network/gig/multi_call/stream/GigClientStreamImpl.h"
#include "aios/network/gig/multi_call/stream/GigStreamRequest.h"

using namespace std;

namespace multi_call {

AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, GigServerStream);

GigClientStream::GigClientStream(const std::string& bizName, const std::string& methodName)
    : RequestGenerator(bizName, INVALID_SOURCE_ID, INVALID_VERSION_ID)
    , _impl(new GigClientStreamImpl(this))
    , _methodName(methodName)
    , _timeout(DEFAULT_TIMEOUT)
    , _hasError(false)
    , _forceStop(false)
{
}

GigClientStream::~GigClientStream() { delete _impl; }

GigClientStreamImpl *GigClientStream::getImpl() { return _impl; }

void GigClientStream::enablePartId(PartIdTy partId) {
    _partIds.insert(partId);
}

const std::set<PartIdTy> &GigClientStream::getPartIds() const {
    return _partIds;
}

PartIdTy GigClientStream::getPartCount() const {
    return _impl->getPartCount();
}

GigStreamRpcInfo GigClientStream::getStreamRpcInfo(PartIdTy partId) const {
    return _impl->getStreamRpcInfo(partId);
}

bool GigClientStream::hasError() const {
    return _hasError;
}

void GigClientStream::generate(PartIdTy partCnt, PartRequestMap &requestMap) {
    const auto &partIds = getPartIds();
    if (partIds.empty()) {
        for (int32_t partId = 0; partId < partCnt; partId++) {
            createRequest(partId, requestMap);
        }
    } else {
        for (auto partId : partIds) {
            if (partId >= partCnt) {
                AUTIL_LOG(ERROR,
                          "generate failed, invalid partId: %d, partCount: %d",
                          partId, partCnt);
                _hasError = true;
                requestMap.clear();
                return;
            }
            createRequest(partId, requestMap);
        }
    }
}

void GigClientStream::createRequest(PartIdTy partId, PartRequestMap &requestMap) {
    auto request = new GigStreamRequest(getProtobufArena());
    request->setStream(this);
    request->setMethodName(_methodName);
    request->setTimeout(_timeout);
    request->disableProbe(getDisableProbe());
    requestMap.emplace(partId, RequestPtr(request));
}

bool GigClientStream::send(PartIdTy partId, bool eof,
                           google::protobuf::Message *message) {
    return _impl->send(partId, eof, message);
}

void GigClientStream::sendCancel(PartIdTy partId,
                                 google::protobuf::Message *message) {
    _impl->sendCancel(partId, message);
}

} // namespace multi_call
