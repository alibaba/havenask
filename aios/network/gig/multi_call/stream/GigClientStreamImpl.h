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
#ifndef ISEARCH_MULTI_CALL_GIGCLIENTSTREAMIMPL_H
#define ISEARCH_MULTI_CALL_GIGCLIENTSTREAMIMPL_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/Caller.h"
#include "aios/network/gig/multi_call/stream/GigStreamMessage.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"
#include "autil/Lock.h"

namespace multi_call {

class SearchServiceResource;
class GigClientStream;
class GigStreamRequest;

class GigClientStreamImpl
{
public:
    GigClientStreamImpl(GigClientStream *stream);
    ~GigClientStreamImpl();

private:
    GigClientStreamImpl(const GigClientStreamImpl &);
    GigClientStreamImpl &operator=(const GigClientStreamImpl &);

public:
    bool init(const std::shared_ptr<ChildNodeReply> &reply,
              const std::vector<std::shared_ptr<SearchServiceResource>> &resourceVec,
              const CallerPtr &caller, bool disableRetry, bool forceStop);
    PartIdTy getPartCount() const;
    bool send(PartIdTy partId, bool eof, google::protobuf::Message *message);
    void sendCancel(PartIdTy partId, google::protobuf::Message *message);
    GigStreamRpcInfo getStreamRpcInfo(PartIdTy partId) const;

private:
    void abort();
    bool writeMessage(bool cancel, const GigStreamMessage &message);

private:
    GigClientStream *_stream;
    std::unordered_map<PartIdTy, std::shared_ptr<GigStreamRequest>> _requestMap;
    PartIdTy _partCount;
    std::shared_ptr<ChildNodeReply> _reply;
    CallerPtr _caller;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigClientStreamImpl);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGCLIENTSTREAMIMPL_H
