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
#include "ha3/service/QrsArpcSession.h"

#ifdef AIOS_OPEN_SOURCE
#include <aios/network/opentelemetry/core/Span.h>
#else
#include <Span.h>
#endif

#include <cstddef>
#include <map>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/XMLResultFormatter.h"
#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/monitor/QrsBizMetrics.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/service/QrsSearcherHandler.h"
#include "ha3/service/QrsSessionSearchRequest.h"
#include "ha3/service/QrsSessionSearchResult.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "ha3/util/TypeDefine.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/search/SearchContext.h"
#include "tensorflow/core/protobuf/config.pb.h"

namespace isearch {
namespace service {
class SessionPool;
}  // namespace service
}  // namespace isearch

using namespace std;
using namespace kmonitor;

using namespace isearch::common;
using namespace isearch::monitor;
using namespace isearch::proto;
using namespace isearch::turing;
namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, QrsArpcSession);

QrsArpcSession::QrsArpcSession(SessionPool* pool)
    : Session(pool)
{}

QrsArpcSession::~QrsArpcSession() {
    reset();
}

void QrsArpcSession::dropRequest() {
    QrsSessionSearchResult result("Server is too busy, thread pool queue is full\n");
    endQrsSession(result);
}
void QrsArpcSession::reset() {
    _snapshot.reset();
    _qrsResponse = NULL;
    _done = NULL;
    _queryStr.clear();
    _clientAddress.clear();
    _bizName.clear();
    _timeout = std::numeric_limits<int64_t>::max();
    POOL_DELETE_CLASS(_timeoutTerminator);
    _timeoutTerminator = NULL;
    recycleRunGraphResource();
    _useFirstBiz = true;
    Session::reset();
}
void QrsArpcSession::setRequest(const proto::QrsRequest *request,
                                proto::QrsResponse *response,
                                RPCClosure *done,
                                const turing::QrsServiceSnapshotPtr &snapshot)
{
    _qrsResponse = response;
    _done = dynamic_cast<multi_call::GigClosure *>(done);
    assert(_done);
    _snapshot = snapshot;
    if (request->has_timeout()) {
        _timeout = request->timeout() * 1000;
    }
    if (request->has_assemblyquery()) {
        _queryStr = request->assemblyquery();
    }
    if (request->has_bizname()) {
        _bizName = request->bizname();
        if (!_bizName.empty()) {
            _useFirstBiz = false;
        }
    }
    _protocolType = request->protocoltype();
}


bool QrsArpcSession::beginSession() {
    if (!Session::beginSession()) {
        return false;
    }
    if (_snapshot == nullptr) {
        AUTIL_LOG(WARN, "snapshot not setted");
        return false;
    }
    _bizName = getBizName(_bizName);
    // bizName not work, if not found, return first biz in map
    turing::QrsBizPtr qrsBiz = dynamic_pointer_cast<turing::QrsBiz>(_snapshot->getBiz(_bizName));
    if (qrsBiz == nullptr) {
        qrsBiz = dynamic_pointer_cast<turing::QrsBiz>(_snapshot->getBiz(_bizName + HA3_LOCAL_QRS_BIZ_NAME_SUFFIX));
    }
    if (qrsBiz == nullptr && _useFirstBiz) {
        qrsBiz = _snapshot->getFirstQrsBiz();
    }
    if (!qrsBiz) {
        AUTIL_LOG(WARN, "get qrs biz [%s] failed", _bizName.c_str());
        return false;
    }
    _bizName = qrsBiz->getBizNameInSnapshot();
    QrsRunGraphContextArgs args = qrsBiz->getQrsRunGraphContextArgs();
    _runId = _runIdAllocator->get();
    args.runOptions.mutable_run_id()->set_value(_runId);
    args.runOptions.set_inter_op_thread_pool(-1);
    args.pool = getMemPool();
    _runGraphContext.reset(new QrsRunGraphContext(args));
    _runGraphContext->setSharedObject(_snapshot->getRpcServer());  //设置rpcServer给qrs调用searcher
    auto queryResource = qrsBiz->prepareQueryResource(getMemPool());
    queryResource->setGigQuerySession(_gigQuerySession);
    auto sessionResource = qrsBiz->getSessionResource();
    if(!sessionResource->addQueryResource(_runId, queryResource)) {
        AUTIL_LOG(WARN, "add query resource failed, runid = %ld", _runId);
        return false;
    }
    _runGraphContext->setQueryResource(queryResource.get());
    return true;
}

void QrsArpcSession::processRequest() {
    AUTIL_LOG(DEBUG, "QrsArpcSession::handleSearchClientRequest");
    if (!beginSession()) {
        string errorMsg = "Qrs begin session failed!";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        ErrorResult errorResult(ERROR_BEGIN_SESSION_FAILED, errorMsg);
        string errorXMLMsg = XMLResultFormatter::xmlFormatErrorResult(errorResult);
        QrsSessionSearchResult qrsSessionSearchResult(errorXMLMsg);
        endQrsSession(qrsSessionSearchResult);
        return;
    }

    int64_t currentTime = autil::TimeUtility::currentTime();
    if (!initTimeoutTerminator(currentTime)) {
        string errorMsg = "Qrs wait in queue timeout!";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        ErrorResult errorResult(ERROR_QRS_OUTPOOLQUEUE_TIMEOUT, errorMsg);
        string errorXMLMsg = XMLResultFormatter::xmlFormatErrorResult(errorResult);
        QrsSessionSearchResult qrsSessionSearchResult(errorXMLMsg);
        endQrsSession(qrsSessionSearchResult);
        return;
    }
    assert(_timeoutTerminator);
    QrsSessionSearchRequest request(_queryStr, _clientAddress,
                                    getMemPool(), _sessionMetricsCollectorPtr);
    request.sessionSrcType = _sessionSrcType;
    QrsSessionSearchResult result;
    {
        const string& partitionIdStr = ProtoClassUtil::partitionIdToString(_snapshot->getPid());
        QrsSearcherHandler searcherHandler(partitionIdStr, _snapshot, _runGraphContext, _timeoutTerminator);
        try {
            result = searcherHandler.process(request, _protocolType, _bizName);
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "EXCEPTION: %s", e.what());
            result = QrsSessionSearchResult(
                    XMLResultFormatter::xmlFormatErrorResult(ErrorResult(ERROR_UNKNOWN, e.what())));
        } catch (...) {
            AUTIL_LOG(ERROR, "UnknownException");
            result = QrsSessionSearchResult(
                    XMLResultFormatter::xmlFormatErrorResult(ErrorResult(ERROR_UNKNOWN, "UnknownException")));
        }
    }
    endQrsSession(result);
}

// in failSession zoneResource and bizInfoManager is null ptr, should be checked!
void QrsArpcSession::endQrsSession(QrsSessionSearchResult &result) {
    Session::endSession();
    fillResponse(result.resultStr, result.resultCompressType,
                 result.formatType, _qrsResponse);

    MetricsReporterPtr metricsReporter;
    MetricsReporterPtr oldMetricsReporter;
    if (_snapshot) {
        turing::QrsBizPtr qrsBiz = dynamic_pointer_cast<turing::QrsBiz>(_snapshot->getBiz(_bizName));
        if (qrsBiz) {
            string srcStr = result.srcStr.empty() ? "empty" : result.srcStr;
            map<string, string> tagsMap = {
                {"src", srcStr},
                {"session_src", transSessionSrcType(_sessionSrcType)}
            };
            suez::turing::SearchContext::addGigMetricTags(_gigQuerySession.get(), tagsMap, _useGigSrc);
            MetricsTags tags(tagsMap);
            metricsReporter = qrsBiz->getQueryMetricsReporter(tags, "kmon");
            oldMetricsReporter = qrsBiz->getQueryMetricsReporter({});
        }
    }
    reportMetrics(metricsReporter.get());
    reportMetrics(oldMetricsReporter.get());
    if (_gigQuerySession) {
        endGigTrace(result);
    }
    if (_done) {
        _done->setErrorCode(result.multicallEc);
        _done->Run();
    }
    destroy();
}

void QrsArpcSession::endGigTrace(QrsSessionSearchResult &result) {
    auto span = _gigQuerySession->getTraceServerSpan();
    if (span != nullptr) {
        span->setAttribute(opentelemetry::kSpanAttrRespContentLength, std::to_string(_qrsResponse->ByteSize()));
        span->setStatus(result.errorStr.empty() ? opentelemetry::StatusCode::kOk : opentelemetry::StatusCode::kError, result.errorStr);
    }
}

void QrsArpcSession::recycleRunGraphResource() {
    _runGraphContext.reset();
    if (_runIdAllocator && _runId >= 0) {
        _runIdAllocator->put(_runId);
        _runIdAllocator.reset();
        _runId = -1;
    }
}

void QrsArpcSession::reportMetrics(MetricsReporter *metricsReporter) {
    if (_sessionMetricsCollectorPtr && metricsReporter) {
        metricsReporter->report<QrsBizMetrics, SessionMetricsCollector>(nullptr,
                _sessionMetricsCollectorPtr.get());
    }
}

void QrsArpcSession::fillResponse(const std::string &result,
                                  autil::CompressType compressType,
                                  ResultFormatType formatType,
                                  proto::QrsResponse *response)
{
    std::string compressedResult;
    bool ret = false;
    if (autil::CompressType::NO_COMPRESS != compressType && getMemPool()) {
        _sessionMetricsCollectorPtr->resultCompressStartTrigger();
        ret = autil::CompressionUtil::compress(result,
                compressType, compressedResult, getMemPool());
        _sessionMetricsCollectorPtr->resultCompressEndTrigger();

        if (ret) {
            _sessionMetricsCollectorPtr->setIsCompress(true);
            _sessionMetricsCollectorPtr->resultCompressLengthTrigger(compressedResult.size());
            response->set_compresstype(proto::ProtoClassUtil::convertCompressType(
                            compressType));
            response->set_assemblyresult(compressedResult);
        }
    }

    if (response) {
        if (autil::CompressType::NO_COMPRESS == compressType || !ret) {
            response->set_compresstype(proto::CT_NO_COMPRESS);
            response->set_assemblyresult(result);
        }
        response->set_formattype(convertFormatType(formatType));
    }
}

void QrsArpcSession::setDropped() {
    if (_sessionMetricsCollectorPtr) {
        _sessionMetricsCollectorPtr->increaseProcessErrorQps();
    }
}

bool QrsArpcSession::initTimeoutTerminator(int64_t currentTime) {
    if (currentTime - getStartTime() >= _timeout) {
        AUTIL_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
        return false;
    }
    if (_timeoutTerminator) {
        POOL_DELETE_CLASS(_timeoutTerminator);
        _timeoutTerminator = NULL;
    }
    _timeoutTerminator = POOL_NEW_CLASS(getMemPool(), TimeoutTerminator, _timeout, getStartTime());
    _timeoutTerminator->init(1);
    AUTIL_LOG(DEBUG, "init timeout terminator, start time [%ld], current time [%ld], "
                "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
    return true;
}

string QrsArpcSession::getBizName(const std::string &bizName) {
    string ret = "";
    if (bizName.empty()) {
        ret = _snapshot->getBizName(DEFAULT_BIZ_NAME, {});
    } else {
        ret = _snapshot->getBizName(bizName, {});
    }
    return ret;
}

} // namespace service
} // namespace isearch
