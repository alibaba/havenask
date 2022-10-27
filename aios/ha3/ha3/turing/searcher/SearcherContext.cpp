#include <suez/turing/common/SessionResource.h>
#include <suez/turing/common/QueryResource.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/searcher/SearcherContext.h>
#include <ha3/common/CommonDef.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/monitor/SearcherBizMetrics.h>
#include <ha3/service/RpcContextUtil.h>
#include <sstream>

using namespace std;
using namespace suez::turing;
using namespace tensorflow;
using namespace kmonitor;
using namespace autil;

USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SearcherContext);

SearcherContext::SearcherContext(const SearchContextArgs &args,
                                 const GraphRequest *request,
                                 GraphResponse *response)
    : GraphSearchContext(args, request, response){
}

SearcherContext::~SearcherContext() {
}

BasicMetricsCollectorPtr SearcherContext::createMetricsCollector() {
    HA3_NS(monitor)::SessionMetricsCollector *collector = new monitor::SessionMetricsCollector();
    collector->setSessionStartTime(TimeUtility::currentTime());
    collector->beginSessionTrigger();
    BasicMetricsCollectorPtr collectorPtr(collector);
    return collectorPtr;
}

string SearcherContext::getBizMetricName() {
    string bizName = _request->bizName;
    size_t pos = bizName.find(".");
    if (pos != string::npos) {
        return bizName.substr(pos + 1);
    } else {
        return bizName;
    }
}

std::string SearcherContext::getRequestSrc() {
    string src = _request->src;
    if (src.empty()) {
        src = "unknown";
    }
    return src;
}

void SearcherContext::prepareQueryMetricsReporter(const BizPtr &biz) {
    {
        map<string, string> tagsMap = {
            {"src", getRequestSrc()},
            {"run_mode", toString(_runMode)},
            {"role_type", "searcher"}
        };
        addGigMetricTags(_gigQuerySession, tagsMap, _useGigSrc);
        MetricsTags tags(tagsMap);
        _queryMetricsReporter = biz->getQueryMetricsReporter(tags, "kmon");
    }
    {
        map<string, string> tagsMap = {
            {"role_type", "searcher"}
        };
        MetricsTags tags(tagsMap);
        _oldQueryMetricsReporter = biz->getQueryMetricsReporter(tags);
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

void SearcherContext::doFormatResult() {
    if (!hasError() && _outputs.size() > 0) {
        auto resultTensor = _outputs[0];
        auto resultVariant = resultTensor.scalar<Variant>()().get<Ha3ResultVariant>();
        if (resultVariant) {
            ResultPtr result = resultVariant->getResult();
            SessionMetricsCollector *collector = dynamic_cast<SessionMetricsCollector*>(_metricsCollector.get());
            if (collector) {
                fillSearchInfo(collector, result);
                SessionMetricsCollector::RequestType requestType = collector->getRequestType();
                if (requestType == SessionMetricsCollector::IndependentPhase1) {
                    auto matchDocs = result->getMatchDocs();
                    if (matchDocs) {
                        collector->returnCountTrigger(matchDocs->size());
                    }

                    if(0 == result->getTotalMatchDocs()) {
                        collector->increaseSearchPhase1EmptyQps();
                    }
                }
            }
        }
    }
    GraphSearchContext::doFormatResult();
}

void SearcherContext::fillMetrics() {
    SearchContext::fillMetrics();
    SessionMetricsCollectorPtr collector = dynamic_pointer_cast<SessionMetricsCollector>(_metricsCollector);
    if (collector == NULL) {
        AUTIL_LOG(INFO, "session metrics collector is empty, base [%ld]", (int64_t)_metricsCollector.get());
        return;
    }
    if (_queryResource) {
        SearcherQueryResourcePtr searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource> (_queryResource);
        if (searcherQueryResource && searcherQueryResource->commonResource) {
            auto &allocator = searcherQueryResource->commonResource->matchDocAllocator;
            if (allocator) {
                collector->matchDocSizeTrigger(allocator->getDocSize());
            }
        }
    }
    if (_pool) {
        collector->setMemPoolUsedBufferSize(_pool->getAllocatedSize());
        collector->setMemPoolAllocatedBufferSize(_pool->getTotalBytes());
    }
}

void SearcherContext::addEagleInfo() {
    if (_gigQuerySession == nullptr) {
        return;
    }
    auto rpcContext = _gigQuerySession->getRpcContext();
    if (rpcContext == nullptr) {
        return;
    }
    string methodName = rpcContext->getUserData(HA_RESERVED_METHOD);
    string pidStr = rpcContext->getUserData(HA_RESERVED_PID);
    PartitionID pid;
    bool ret = pid.ParseFromString(pidStr);
    if (ret) {
        rpcContext->serverBegin(RpcContextUtil::getRpcRole(pid), methodName);
    }
}

void SearcherContext::reportMetrics() {
    SearchContext::reportMetrics();
    if (_queryMetricsReporter) {
        auto *collector = dynamic_cast<SessionMetricsCollector*>(_metricsCollector.get());
        if (collector == nullptr) {
            HA3_LOG(DEBUG, "dynamic cast metrics collector into ha3 collector failed.");
            return;
        }
        _queryMetricsReporter->report<SearcherBizMetrics, SessionMetricsCollector>(nullptr, collector);
    }
    if (_oldQueryMetricsReporter) {
        auto *collector = dynamic_cast<SessionMetricsCollector*>(_metricsCollector.get());
        if (collector == nullptr) {
            return;
        }
        _oldQueryMetricsReporter->report<SearcherBizMetrics, SessionMetricsCollector>(nullptr, collector);
    }
}

void SearcherContext::formatErrorResult() {
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
        const proto::PartitionID &partitionId = searcherSessionResource->searcherResource->getPartitionId();
        result->addCoveredRange(partitionId.clustername(),
                                partitionId.range().from(),
                                partitionId.range().to());
    }
    auto resultTensor = Tensor(DT_VARIANT, TensorShape({}));
    Ha3ResultVariant ha3Result(result, _pool);

    resultTensor.scalar<Variant>()() = ha3Result;

    suez::turing::NamedTensorProto *namedTensor = _graphResponse->add_outputs();
    namedTensor->set_name(HA3_RESULT_TENSOR_NAME);
    resultTensor.AsProtoField(namedTensor->mutable_tensor());
}

END_HA3_NAMESPACE(turing);
