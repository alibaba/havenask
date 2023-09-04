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
#include "suez/service/WriteDone.h"

#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "autil/Scope.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "suez/sdk/TableWriter.h"
#include "suez/service/TableServiceHelper.h"
using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace autil::result;

namespace suez {
AUTIL_LOG_SETUP(suez, WriteDone);

static DocWriteState convertState(WriterState s) {
    switch (s) {
    case WriterState::LOG:
        return DocWriteState::LOG;
    case WriterState::ASYNC:
        return DocWriteState::ASYNC;
    case WriterState::SYNC:
        return DocWriteState::SYNC;
    default:
        return DocWriteState::ERROR;
    }
}

WriteDone::WriteDone(const WriteRequest *request,
                     WriteResponse *response,
                     google::protobuf::Closure *rpcDone,
                     kmonitor::MetricsReporter *metricsReporter,
                     std::shared_ptr<WriteTableAccessLog> accessLog,
                     int64_t startTime,
                     shared_ptr<IndexProvider> indexProvider,
                     int totalDocCount)
    : _request(request)
    , _response(response)
    , _rpcDone(rpcDone)
    , _metricsReporter(metricsReporter)
    , _accessLog(accessLog)
    , _startTime(startTime)
    , _states(response->mutable_docwritestate())
    , _indexProvider(indexProvider) {
    _states->Resize(totalDocCount, DocWriteState::LOG);
}

WriteDone::~WriteDone() {
    if (!_errors.empty()) {
        string message = StringUtil::toString(_errors, ", ");
        TableServiceHelper::trySetGigEc(_rpcDone, multi_call::MULTI_CALL_REPLY_ERROR_RESPONSE);
        TableServiceHelper::setErrorInfo(_response->mutable_errorinfo(),
                                         _rpcDone,
                                         TBS_ERROR_OTHERS,
                                         message,
                                         kWrite,
                                         _request->tablename(),
                                         _metricsReporter);
    } else if (_metricsReporter) {
        MetricsTags baseTags = {{{kQueryType, kWrite}, {kTableName, _request->tablename()}}};
        int64_t latency = TimeUtility::currentTime() - _startTime;
        _metricsReporter->report(latency, kLatency, kmonitor::GAUGE, &baseTags);
        _metricsReporter->report(1, kQps, kmonitor::QPS, &baseTags);
    }
    _accessLog->collectResponse(_response);
    _rpcDone->Run();
}
void WriteDone::run(Result<WriteResult> result, vector<int> docList) {
    unique_lock<mutex> lock(_mu);
    if (result.is_ok()) {
        auto writeResult = std::move(result).steal_value();
        for (auto &docIndex : docList) {
            _states->Set(docIndex, convertState(writeResult.state));
        }
        // response->buildGap = max(buildGap)
        if (_response->has_buildgap()) {
            if (_response->buildgap() < writeResult.watermark.buildGap) {
                _response->set_buildgap(writeResult.watermark.buildGap);
            }
        } else {
            _response->set_buildgap(writeResult.watermark.buildGap);
        }
        // response->checkpoint = max(maxCp)
        if (_response->has_checkpoint()) {
            if (_response->checkpoint() < writeResult.watermark.maxCp) {
                _response->set_checkpoint(writeResult.watermark.maxCp);
            }
        } else {
            _response->set_checkpoint(writeResult.watermark.maxCp);
        }
        _accessLog->collectWriteResult(std::move(writeResult));
    } else {
        _errors.push_back(result.get_error().message());
        for (auto &docIndex : docList) {
            _states->Set(docIndex, DocWriteState::ERROR);
        }
    }
}
} // namespace suez
