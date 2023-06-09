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
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "suez/sdk/PartitionId.h"
#include "suez/turing/common/CommonDefine.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/common/TuringRequest.h"
#include "suez/turing/metrics/BasicBizMetrics.h"
#include "suez/turing/proto/ErrorCode.pb.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/Biz.h"
#include "suez/turing/search/GraphSearchContext.h"
#include "suez/turing/search/SearchContext.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"

#include "ha3/common/CommonDef.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"
#include "ha3/common/searchinfo/PhaseTwoSearchInfo.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SearcherBizMetrics.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/TraceSpanUtil.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/searcher/SearcherContext.h"
#include "ha3/turing/searcher/SearcherServiceSnapshot.h"
#include "ha3/turing/qrs/QrsServiceSnapshot.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h"

using namespace std;
using namespace suez::turing;
using namespace tensorflow;
using namespace kmonitor;
using namespace autil;

using namespace isearch::proto;
using namespace isearch::monitor;
using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::service;
namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, SearcherContext);

SearcherContext::SearcherContext(const SearchContextArgs &args,
                                 const GraphRequest *request,
                                 GraphResponse *response)
    : GraphSearchContext(args, request, response)
{
}

SearcherContext::~SearcherContext() {
}

void SearcherContext::init() {
    _sessionMetricsCollector.reset(new monitor::SessionMetricsCollector());
    _sessionMetricsCollector->setSessionStartTime(TimeUtility::currentTime());
    _sessionMetricsCollector->beginSessionTrigger();
    addEagleInfo();
}

void SearcherContext::addExtraTags(map<string, string> &tagsMap) {
    tagsMap["role_type"] = "searcher";
}

void SearcherContext::fillQueryResource() {
    SearchContext::fillQueryResource();
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(_queryResource);
    if (searcherQueryResource) {
        searcherQueryResource->sessionMetricsCollector = _sessionMetricsCollector.get();
        auto ha3ServiceSnapshot =
            dynamic_cast<Ha3ServiceSnapshot*>(_serviceSnapshot.get());
        if (ha3ServiceSnapshot) {
            searcherQueryResource->ha3BizMeta = ha3ServiceSnapshot->getHa3BizMeta();
        }
    }
}

#define GREATER_THAN_ZERO(value) ((value) > 0 ? (value) : 0)

void SearcherContext::fillSearchInfo(SessionMetricsCollector *collector, ResultPtr &result) {
    collector->endSessionTrigger();
    SessionMetricsCollector::RequestType requestType =
        collector->getRequestType();
    if (requestType == SessionMetricsCollector::IndependentPhase1) {
        PhaseOneSearchInfo *searchInfo = new PhaseOneSearchInfo;
        searchInfo->partitionCount = 1;
        searchInfo->useTruncateOptimizerCount =
            collector->useTruncateOptimizer() ? 1 : 0;
        searchInfo->fromFullCacheCount =
            collector->isCacheHit() ? 1 : 0;
        searchInfo->seekCount = GREATER_THAN_ZERO(collector->getSeekCount());
        searchInfo->matchCount = GREATER_THAN_ZERO(collector->getMatchCount());
        searchInfo->seekDocCount = GREATER_THAN_ZERO(collector->getSeekDocCount());
        searchInfo->rankLatency =
            GREATER_THAN_ZERO(collector->getRankLatencyInUs());
        searchInfo->rerankLatency =
            GREATER_THAN_ZERO(collector->getReRankLatencyInUs());
        searchInfo->extraLatency =
            GREATER_THAN_ZERO(collector->getExtraRankLatencyInUs());
        searchInfo->searcherProcessLatency =
            GREATER_THAN_ZERO(collector->getProcessLatencyInUs());
	searchInfo->otherInfoStr = collector->getOtherInfoStr();
        result->setPhaseOneSearchInfo(searchInfo);
    } else {
        // phaseTwoSearchInfo fill to PhaseOneSearchInfo.otherInfoStr for compatibility
        result->setPhaseOneSearchInfo(new PhaseOneSearchInfo);
        PhaseTwoSearchInfo *searchInfo = new PhaseTwoSearchInfo;
        searchInfo->summaryLatency =
            GREATER_THAN_ZERO(collector->getProcessLatencyInUs());
        result->setPhaseTwoSearchInfo(searchInfo);
    }
}

void SearcherContext::collectMetricAndSearchInfo() {
    if (hasError() || _outputs.empty()) {
        return;
    }
    auto resultVariant = _outputs[0].scalar<Variant>()().get<Ha3ResultVariant>();
    if (!resultVariant) {
        return;
    }
    ResultPtr result = resultVariant->getResult();
    SessionMetricsCollector *collector = dynamic_cast<SessionMetricsCollector*>(
            _sessionMetricsCollector.get());
    if (collector) {
        fillSearchInfo(collector, result);
        SessionMetricsCollector::RequestType requestType = collector->getRequestType();
        if (requestType == SessionMetricsCollector::IndependentPhase1) {
            auto matchDocs = result->getMatchDocs();
            if (matchDocs) {
                collector->returnCountTrigger(matchDocs->size());
            }

            // if all matchdocs are deleted by score/sort plugin and really do aggregate,
            // it will not be treated as an empty qps, but 'resultCount' metrics remain working
            if(0 == result->getTotalMatchDocs() && collector->getAggregateCount() <= 0) {
                collector->increaseSearchPhase1EmptyQps();
            }
        }
    }
}

void SearcherContext::doFormatResult() {
    collectMetricAndSearchInfo();
    GraphSearchContext::doFormatResult();
}

void SearcherContext::addEagleInfo() {
    if (_gigQuerySession == nullptr) {
        return;
    }
    string methodName = _gigQuerySession->getUserData(HA_RESERVED_METHOD);
    string pidStr = _gigQuerySession->getUserData(HA_RESERVED_PID);
    PartitionID pid;
    bool ret = pid.ParseFromString(pidStr);
    if (ret) {
        auto span = _gigQuerySession->getTraceServerSpan();
        if (span) {
            span->setAttribute(opentelemetry::kEagleeyeServiceName, TraceSpanUtil::getRpcRole(pid));
            span->setAttribute(opentelemetry::kEagleeyeMethodName, methodName);
        }
    }
}

void SearcherContext::reportMetrics() {
    GraphSearchContext::reportMetrics();
    if (_queryResource) {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(_queryResource);
        if (searcherQueryResource && searcherQueryResource->commonResource) {
            auto &allocator = searcherQueryResource->commonResource->matchDocAllocator;
            if (allocator) {
                _sessionMetricsCollector->matchDocSizeTrigger(allocator->getDocSize());
            }
        }
    }
    if (_pool) {
        _sessionMetricsCollector->setMemPoolUsedBufferSize(_pool->getAllocatedSize());
        _sessionMetricsCollector->setMemPoolAllocatedBufferSize(_pool->getTotalBytes());
    }

    if (_queryMetricsReporter) {
        _queryMetricsReporter->report<SearcherBizMetrics, SessionMetricsCollector>(
                nullptr, _sessionMetricsCollector.get());
    }
    if (_oldQueryMetricsReporter) {
        _oldQueryMetricsReporter->report<SearcherBizMetrics, SessionMetricsCollector>(
                nullptr, _sessionMetricsCollector.get());
    }
}

void SearcherContext::formatErrorResult() {
    _graphResponse->set_multicall_ec(multi_call::MULTI_CALL_REPLY_ERROR_RESPONSE);
    const vector<string> &outputNames = _request->outputNames;
    if (outputNames.size() != 1 || outputNames[0] != HA3_RESULT_TENSOR_NAME) {
        return;
    }
    SearcherSessionResource* searcherSessionResource
        = dynamic_cast<SearcherSessionResource *> (_sessionResource.get());
    if (!searcherSessionResource) {
        return;
    }
    SearcherQueryResource* searcherQueryResource = dynamic_cast<SearcherQueryResource *> (_queryResource.get());
    if (!searcherQueryResource) {
        return;
    }
    SearchCommonResourcePtr commonResource = searcherQueryResource->commonResource;
    if (!commonResource) {
        return;
    }
    common::ResultPtr result = common::ResultPtr(new common::Result(new common::MatchDocs()));
    ErrorResult errResult(ERROR_RUN_SEARCHER_GRAPH_FAILED, "run searcher graph failed: " +
                          _errorInfo.errormsg());
    result->addErrorResult(errResult);
    result->addErrorResult(commonResource->errorResult);
    if (searcherQueryResource->getTracerPtr()) {
        result->setTracer(searcherQueryResource->getTracerPtr());
    }
    if (searcherSessionResource->searcherResource) {
        string clusterName = searcherSessionResource->searcherResource->getClusterName();
        auto range = searcherQueryResource->partRange;
        result->addCoveredRange(clusterName, range.first, range.second);
    }
    auto resultTensor = Tensor(DT_VARIANT, TensorShape({}));
    Ha3ResultVariant ha3Result(result, _pool);
    resultTensor.scalar<Variant>()() = ha3Result;
    suez::turing::NamedTensorProto *namedTensor = _graphResponse->add_outputs();
    namedTensor->set_name(HA3_RESULT_TENSOR_NAME);
    resultTensor.AsProtoField(namedTensor->mutable_tensor());
}

} // namespace turing
} // namespace isearch
