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
#ifndef ISEARCH_MULTI_CALL_GIGCLIENTSTREAM_H
#define ISEARCH_MULTI_CALL_GIGCLIENTSTREAM_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "aios/network/gig/multi_call/stream/GigStreamBase.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace multi_call {

class SearchService;
typedef std::map<PartIdTy, google::protobuf::Message *> PartMessageMap;
class GigClientStreamImpl;
struct ControllerFeedBack;

class GigClientStream : public GigStreamBase, public RequestGenerator {
public:
    GigClientStream(const std::string &bizName, const std::string &methodName);
    virtual ~GigClientStream();

private:
    GigClientStream(const GigClientStream &);
    GigClientStream &operator=(const GigClientStream &);

public:
    bool send(PartIdTy partId, bool eof,
              google::protobuf::Message *message) override;
    void sendCancel(PartIdTy partId,
                    google::protobuf::Message *message) override;
public:
    void generate(PartIdTy partCnt, PartRequestMap &requestMap) override final;
    bool hasError() const override final;
private:
    void createRequest(PartIdTy partId, PartRequestMap &requestMap);
public:
    void enablePartId(PartIdTy partId);
    const std::set<PartIdTy> &getPartIds() const;
    PartIdTy getPartCount() const;
    GigStreamRpcInfo getStreamRpcInfo(PartIdTy partId) const;
    void setMethodName(const std::string &methodName) {
        _methodName = methodName;
    }
    const std::string &getMethodName() const { return _methodName; }
    // ms
    void setTimeout(uint64_t timeout) { _timeout = timeout; }
    uint64_t getTimeout() const { return _timeout; }
    void setForceStop(bool force) {
        _forceStop = force;
    }
    bool getForceStop() const {
        return _forceStop;
    }
private:
    GigClientStreamImpl *getImpl();

private:
    friend class SearchService;

private:
    GigClientStreamImpl *_impl;
    std::string _methodName;
    uint64_t _timeout;
    bool _hasError : 1;
    bool _forceStop : 1;
    std::set<PartIdTy> _partIds;
};

MULTI_CALL_TYPEDEF_PTR(GigClientStream);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGCLIENTSTREAM_H
