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
#include "ha3/turing/ops/Ha3AuxScanOp.h"

#include <stdint.h>
#include <cstddef>
#include <map>
#include <memory>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
//#include "basic_ops/common/CommonDefine.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "indexlib/util/metrics/Monitor.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/VirtualAttribute.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/ClauseBase.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/LayerMetasCreator.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "ha3/search/QueryExecutorCreator.h"
#include "ha3/search/RangeQueryExecutor.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/ha3_op_common.h"
#include "ha3/turing/ops/AuxSearchInfo.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil;
using namespace autil::mem_pool;
using namespace matchdoc;

using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::turing;
using namespace isearch::service;

namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(ha3, Ha3AuxScanOp);
static const std::string kOpQpsMetric = "user.op.Qps";
static const std::string kOpLatencyMetric = "user.op.Latency";
static const std::string kOpOutCountMetric = "user.op.Out_Count";

Ha3AuxScanOp::Ha3AuxScanOp(tensorflow::OpKernelConstruction *ctx)
    : tensorflow::OpKernel(ctx)
{
    auto sessionResource = GET_SESSION_RESOURCE();
    SearcherSessionResource *searcherSessionResource =
        dynamic_cast<SearcherSessionResource *>(sessionResource);
    OP_REQUIRES(ctx,
        searcherSessionResource,
        errors::Unavailable("ha3 searcher session resource unavailable"));
    SearcherResourcePtr searcherResource =
        searcherSessionResource->searcherResource;
    OP_REQUIRES(ctx,
        searcherResource,
        errors::Unavailable("empty searcher resource"));
    const auto &joinConfig =
        searcherResource->getClusterConfig().getJoinConfig();
    _tableName = joinConfig.getScanJoinCluster();
    OP_REQUIRES(ctx,
        !_tableName.empty(),
        errors::Unavailable("empty scan join cluster name"));
    const auto &joinInfos = joinConfig.getJoinInfos();
    for (const auto &ji : joinInfos) {
        if (_tableName == ji.getJoinCluster()) {
	    _joinFieldName = ji.getJoinField();
            AUTIL_LOG(INFO,
                "get join cluster:  [%s] - [%s]",
                _tableName.c_str(),
                _joinFieldName.c_str());
            break;
        }
    }
    if (_joinFieldName.empty()) {
        OP_REQUIRES(ctx,
            false,
            errors::Unavailable(
                "scan join cluster name not in join table info"));
    }
    _tags.AddTag("table_name", _tableName);
    ADD_OP_BASE_TAG(_tags);
}

Ha3AuxScanOp::~Ha3AuxScanOp() {
}

void Ha3AuxScanOp::Compute(tensorflow::OpKernelContext* ctx) {
    int64_t startTime = TimeUtility::currentTime();
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherSessionResource *searcherSessionResource =
        dynamic_cast<SearcherSessionResource *>(sessionResource);
    OP_REQUIRES(ctx, searcherSessionResource,
        errors::Unavailable("ha3 searcher session resource unavailable"));
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
        errors::Unavailable("ha3 searcher query resource unavailable"));
    REQUEST_TRACE_WITH_TRACER(
            queryResource->getTracer(), TRACE3, "begin aux scan op");

    auto userMetricReporter = queryResource->getQueryMetricsReporter();
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto requestVariant = requestTensor.get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant,
	errors::Unavailable("get requestVariant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get request failed"));
    AuxQueryClause *auxQueryClause = request->getAuxQueryClause();
    AuxFilterClause *auxFilterClause = request->getAuxFilterClause();

    if (!auxQueryClause && !auxFilterClause) {
        REQUEST_TRACE_WITH_TRACER(
                queryResource->getTracer(), TRACE3, "no aux query clause or aux filter clause, skip aux scan");
        Tensor *output = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &output));
        output->scalar<Variant>()() = MatchDocsVariant();
	return;
    }

    REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
        TRACE3,
        "begin aux scan, tableName:[%s], joinFieldName:[%s]",
        _tableName.c_str(),
        _joinFieldName.c_str());

    auto* pool = queryResource->getPool();
    MatchDocAllocatorPtr matchDocAllocator(new MatchDocAllocator(pool, false));

    auto tableInfoIter =
	sessionResource->dependencyTableInfoMap.find(_tableName);
    OP_REQUIRES(ctx,
	tableInfoIter != sessionResource->dependencyTableInfoMap.end(),
    	errors::Unavailable("cannot get table info: " + _tableName));
    auto tableInfo = tableInfoIter->second;
    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
            queryResource->getIndexSnapshot(), _tableName);
    SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
    OP_REQUIRES(ctx, searcherResource,
    	errors::Unavailable("cannot get searcher resource"));
    FunctionProviderPtr functionProvider(new FunctionProvider(
                    matchDocAllocator.get(), pool,
                    queryResource->getCavaAllocator(), queryResource->getTracer(),
                    queryResource->getIndexSnapshot(), NULL));
    AttributeExpressionCreatorPtr attributeExpressionCreator(
	new AttributeExpressionCreator(
	    pool,
	    matchDocAllocator.get(),
	    _tableName,
	    queryResource->getIndexSnapshot(),
	    tableInfo,
	    VirtualAttributes(),
	    searcherResource->getFuncCreator().get(),
	    sessionResource->cavaPluginManager.get(),
	    functionProvider.get()));

    QueryPtr auxQueryPtr;
    if (auxQueryClause) {
        auto *rootQuery = auxQueryClause->getRootQuery();
	if (rootQuery) {
	    auxQueryPtr.reset(rootQuery->clone());
            REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
                TRACE3,
                "with aux query: %s",
                auxQueryPtr->toString().c_str());
        }
    }

    if (auxFilterClause) {
        REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
            TRACE3,
            "with filter clause: %s",
            auxFilterClause->getOriginalString().c_str());
    }
    FilterWrapperPtr filterWrapper = createFilterWrapper(auxFilterClause,
	attributeExpressionCreator.get(), matchDocAllocator.get(), pool);
    LayerMetaPtr layerMeta = createLayerMeta(indexPartitionReaderWrapper, pool);
    bool emptyScan = false;
    QueryExecutor *queryExecutor = createQueryExecutor(
            auxQueryPtr, indexPartitionReaderWrapper, _tableName, pool,
            queryResource->getTimeoutTerminator(),
            layerMeta.get(), queryResource->getTracer(), emptyScan);
    OP_REQUIRES(ctx, !(queryExecutor == nullptr && !emptyScan),
                errors::Unavailable("create query executor failed"));
    if (queryExecutor == nullptr && emptyScan) {
        REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
            TRACE3,
            "query is empty, no matched result");
        Tensor *output = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &output));
        MatchDocsVariant matchDocsVariant(matchDocAllocator, pool);
        output->scalar<Variant>()() = matchDocsVariant;
        return;
    }
    QueryExecutorPtr queryExecutorPtr(queryExecutor,
                [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
    shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader
        = indexPartitionReaderWrapper->getDeletionMapReader();
    indexlib::index::DeletionMapReaderPtr subDelMapReader = nullptr;
    indexlib::index::JoinDocidAttributeIterator *mainToSubIt = nullptr;

    if (matchDocAllocator->hasSubDocAllocator()) {
        mainToSubIt = indexPartitionReaderWrapper->getMainToSubIter();
        const indexlib::partition::IndexPartitionReaderPtr &subIndexPartReader
            = indexPartitionReaderWrapper->getSubReader();
        if (subIndexPartReader) {
            subDelMapReader = subIndexPartReader->GetDeletionMapReader();
        }
    }

    search::SingleLayerSearcherPtr singleLayerSearcher(
            new search::SingleLayerSearcher(
                    queryExecutor,
                    layerMeta.get(),
                    filterWrapper.get(),
                    delMapReader.get(),
                    matchDocAllocator.get(),
                    queryResource->getTimeoutTerminator(),
                    mainToSubIt,
                    subDelMapReader.get(),
                    nullptr,
                    false));
    bool needSubDoc = matchDocAllocator->hasSubDocAllocator();
    vector<MatchDoc> matchDocs;
    matchdoc::MatchDoc doc;
    indexlib::index::ErrorCode ec;
    while (true) {
        ec = singleLayerSearcher->seek(needSubDoc, doc);
        if (ec != indexlib::index::ErrorCode::OK) {
            REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
                    TRACE3,
                    "seek error %s", indexlib::index::ErrorCodeToString(ec).c_str());
            break;
        }
        if (matchdoc::INVALID_MATCHDOC == doc) {
            break;
        }
        matchDocs.push_back(doc);
    }
    size_t resultCount = matchDocs.size();
    REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
        TRACE3,
        "aux scan result count: %d", (int32_t)resultCount);
    AttributeExpression *expr =
            attributeExpressionCreator->createAtomicExpr(_joinFieldName);
    if (!expr || !expr->allocate(matchDocAllocator.get())) {
        OP_REQUIRES(ctx, false,
                    errors::Unavailable("create join field attr failed: ",
                                        _joinFieldName));
    }
    matchDocAllocator->extend();
    vector<AttributeExpression*> exprVec;
    exprVec.push_back(expr);
    ExpressionEvaluator<std::vector<AttributeExpression *>> evaluator(
            exprVec, matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(&matchDocs[0], matchDocs.size());
    MatchDocsVariant matchDocsVariant(matchDocAllocator, pool);
    matchDocsVariant.stealMatchDocs(matchDocs);

    Tensor *output = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &output));
    output->scalar<Variant>()() = matchDocsVariant;

    int64_t endTime = TimeUtility::currentTime();
    AuxSearchInfo auxSearchInfo;
    auxSearchInfo._elapsedTime = (endTime - startTime)/1000.0;
    auxSearchInfo._resultCount = (uint32_t)resultCount;

    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;

    string otherInfoStr = auxSearchInfo.toString();
    metricsCollector->addOtherInfoStr(otherInfoStr);
    REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
        TRACE3,
        "aux scan search info: %s", otherInfoStr.c_str());
    if (userMetricReporter) {
        userMetricReporter->report(auxSearchInfo._elapsedTime,
                kOpLatencyMetric, kmonitor::GAUGE, &_tags);
        userMetricReporter->report(auxSearchInfo._resultCount,
                kOpOutCountMetric, kmonitor::GAUGE, &_tags);
        userMetricReporter->report(1, kOpQpsMetric, kmonitor::QPS, &_tags);
    }
}


FilterWrapperPtr Ha3AuxScanOp::createFilterWrapper(
        const AuxFilterClause *auxFilterClause,
        AttributeExpressionCreator *attrExprCreator,
        MatchDocAllocator *matchDocAllocator,
        autil::mem_pool::Pool *pool)
{
    Filter *auxFilter = NULL;
    SubDocFilter *subDocFilter = NULL;
    if (auxFilterClause) {
        auxFilter = Filter::createFilter(auxFilterClause->getRootSyntaxExpr(), attrExprCreator, pool);
        if (auxFilter == NULL) {
            AUTIL_LOG(WARN, "Create AuxFilter failed.");
            return {};
        }
    }
    if (matchDocAllocator->hasSubDocAllocator()) {
        subDocFilter = POOL_NEW_CLASS(pool, SubDocFilter,
                matchDocAllocator->getSubDocAccessor());
    }
    if (auxFilter != NULL || subDocFilter != NULL) {
        FilterWrapperPtr filterWrapper(POOL_NEW_CLASS(pool, FilterWrapper),
	    [](FilterWrapper* p) {
                POOL_DELETE_CLASS(p);
            });
        filterWrapper->setFilter(auxFilter);
        filterWrapper->setSubDocFilter(subDocFilter);
	return filterWrapper;
    }
    return {};
}

LayerMetaPtr Ha3AuxScanOp::createLayerMeta(
        IndexPartitionReaderWrapperPtr &indexPartitionReader,
        autil::mem_pool::Pool *pool)
{
    LayerMetasCreator layerMetasCreator;
    layerMetasCreator.init(pool, indexPartitionReader.get());
    LayerMetasPtr layerMetas(layerMetasCreator.create(NULL),
                             [](LayerMetas* p) {
                POOL_DELETE_CLASS(p);
            });
    LayerMetaPtr layerMeta;
    if (layerMetas == NULL) {
        AUTIL_LOG(INFO, "create layer meta failed.");
        return layerMeta;
    }
    if (layerMetas->size() != 1) {
        AUTIL_LOG(INFO, "layer meta size [%zu] not equal 1.", layerMetas->size());
        return layerMeta;
    }

    for (auto &rangeMeta : (*layerMetas)[0]) {
        rangeMeta.quota =  rangeMeta.end - rangeMeta.begin + 1;
    }
    layerMeta.reset(new LayerMeta((*layerMetas)[0]));
    return layerMeta;
}

isearch::search::QueryExecutor *
Ha3AuxScanOp::createQueryExecutor(const QueryPtr &query,
                                  IndexPartitionReaderWrapperPtr &indexPartitionReader,
                                  const std::string &tableName,
                                  autil::mem_pool::Pool *pool,
                                  autil::TimeoutTerminator *timeoutTerminator,
                                  LayerMeta *layerMeta,
                                  Tracer *tracer,
                                  bool &emptyScan)
{
    emptyScan = false;
    QueryExecutor *queryExecutor = nullptr;
    if (query == nullptr) {
        queryExecutor = POOL_NEW_CLASS(pool, RangeQueryExecutor, layerMeta);
        REQUEST_TRACE_WITH_TRACER(
                tracer, TRACE1,
                "table name [%s], no query executor, null query", tableName.c_str());
        return queryExecutor;
    }
    REQUEST_TRACE_WITH_TRACER(
            tracer, DEBUG, "create query executor, table name [%s], query [%s]",
            tableName.c_str(),
            query->toString().c_str());
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;
    try {
        QueryExecutorCreator qeCreator(
            nullptr, indexPartitionReader.get(), pool, timeoutTerminator, layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
        if (queryExecutor->isEmpty()) {
            POOL_DELETE_CLASS(queryExecutor);
            queryExecutor = NULL;
            emptyScan = true;
        }
    } catch (const indexlib::util::ExceptionBase &e) {
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception &e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_LOOKUP;
    }
    if (errorCode != ERROR_NONE) {
        REQUEST_TRACE_WITH_TRACER(
            tracer, WARN,
            "table name [%s], Create query executor failed, query [%s], Exception: [%s]",
            tableName.c_str(),
            query->toString().c_str(),
            errorMsg.c_str());
    }
    return queryExecutor;
}

REGISTER_KERNEL_BUILDER(Name("Ha3AuxScanOp").Device(tensorflow::DEVICE_CPU),
                        Ha3AuxScanOp);

} // namespace turing
} // namespace isearch
