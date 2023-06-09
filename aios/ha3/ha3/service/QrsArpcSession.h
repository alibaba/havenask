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


#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/RPCServerAdapter.h"
#include "autil/CompressionUtil.h"
#include "google/protobuf/stubs/common.h"
#include "suez/turing/common/RunIdAllocator.h"

#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/proto/QrsService.pb.h"
#include "ha3/service/QrsSessionSearchRequest.h"
#include "ha3/service/QrsSessionSearchResult.h"
#include "ha3/service/Session.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "ha3/turing/qrs/QrsServiceSnapshot.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace service {

class QrsArpcSession : public Session
{
public:
    QrsArpcSession(SessionPool* pool = NULL);
    virtual ~QrsArpcSession();
private:
    QrsArpcSession(const QrsArpcSession &);
    QrsArpcSession& operator=(const QrsArpcSession &);
public:
    void processRequest();
    void dropRequest();
    bool beginSession();
    void setRequest(const proto::QrsRequest *request,
                    proto::QrsResponse *response,
                    RPCClosure *done,
                    const turing::QrsServiceSnapshotPtr &snapshot);
    void endQrsSession(QrsSessionSearchResult &sessionSearchResult);
    void setClientAddress(const std::string &clientAddress) {
        _clientAddress = clientAddress;
    }

    void reset();

    void setDropped();

    void setRunIdAllocator(suez::turing::RunIdAllocatorPtr runIdAllocator) {
        _runIdAllocator = runIdAllocator;
    }
protected:
    void reportMetrics(kmonitor::MetricsReporter *metricsReporter);
    bool isTimeout(int64_t currentTime);
private:
    void recycleRunGraphResource();
    void fillResponse(const std::string &result,
                      autil::CompressType compressType,
                      ResultFormatType formatType,
                      proto::QrsResponse *response);
    std::string getBizName(const std::string &bizName);
    void endGigTrace(QrsSessionSearchResult &result);
private:
    inline proto::FormatType convertFormatType(ResultFormatType formatType);
    bool initTimeoutTerminator(int64_t currentTime);
protected:
    turing::QrsServiceSnapshotPtr _snapshot;
    proto::QrsResponse *_qrsResponse = nullptr;
    multi_call::GigClosure *_done = nullptr;
    // query input
    std::string _queryStr;
    std::string _clientAddress;
    std::string _bizName;
    int64_t _timeout = std::numeric_limits<int64_t>::max();
    common::TimeoutTerminator *_timeoutTerminator = nullptr;
    suez::turing::RunIdAllocatorPtr _runIdAllocator;
    int64_t _runId = -1;
    autil::mem_pool::Pool *_pool = nullptr;
    proto::ProtocolType _protocolType;
    bool _useFirstBiz = true;
private:
    turing::QrsRunGraphContextPtr _runGraphContext;
private:
    friend class QrsArpcSessionTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsArpcSession> QrsArpcSessionPtr;

inline proto::FormatType QrsArpcSession::convertFormatType(ResultFormatType formatType) {
    switch (formatType)
    {
    case RF_PROTOBUF:
        return proto::FT_PROTOBUF;
        break;
    case RF_FB_SUMMARY:
        return proto::FT_FB_SUMMARY;
        break;
    default:
        return proto::FT_XML;
        break;
    }
}

} // namespace service
} // namespace isearch
