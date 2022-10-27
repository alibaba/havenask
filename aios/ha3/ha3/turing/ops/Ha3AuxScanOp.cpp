#include <ha3/turing/ops/Ha3AuxScanOp.h>
#include <ha3/common/Request.h>
#include <ha3/sql/ops/scan/ScanIteratorCreator.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/search/LayerMetasCreator.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/provider/FunctionProvider.h>
#include <ha3/common/ha3_op_common.h>
#include <ha3/turing/ops/AuxSearchInfo.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil;
using namespace autil::mem_pool;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(sql);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, Ha3AuxScanOp);

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
            HA3_LOG(INFO,
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
    	errors::Unavailable("cannot get table info"));
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
	    sessionResource->cavaPluginManager,
	    functionProvider.get()));

    ScanIteratorCreatorParam creatorParam;
    creatorParam.tableName = _tableName;
    creatorParam.indexPartitionReaderWrapper = indexPartitionReaderWrapper;
    creatorParam.attributeExpressionCreator = attributeExpressionCreator;
    creatorParam.matchDocAllocator = matchDocAllocator;
    creatorParam.tableInfo = tableInfo;
    creatorParam.pool = pool;
    creatorParam.timeoutTerminator = queryResource->getTimeoutTerminator();

    ScanIteratorCreator scanIteratorCreator(creatorParam);
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
    ScanIteratorPtr scanIterator(scanIteratorCreator.createScanIterator(
            auxQueryPtr, filterWrapper, layerMeta, emptyScan));
    OP_REQUIRES(ctx, !(!scanIterator && !emptyScan),
                errors::Unavailable("create scan iterator failed"));
    if (!scanIterator) {
        REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(),
            TRACE3,
            "scan iterator is empty, no matched result");
	Tensor *output = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &output));
        MatchDocsVariant matchDocsVariant(matchDocAllocator, pool);
        output->scalar<Variant>()() = matchDocsVariant;
	return;
    }
    vector<MatchDoc> matchDocs;
    while (!scanIterator->batchSeek(0, matchDocs)) {
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
        auxFilter = Filter::createFilter(auxFilterClause, attrExprCreator, pool);
        if (auxFilter == NULL) {
            HA3_LOG(WARN, "Create AuxFilter failed.");
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
        HA3_LOG(INFO, "create layer meta failed.");
        return layerMeta;
    }
    if (layerMetas->size() != 1) {
        HA3_LOG(INFO, "layer meta size [%zu] not equal 1.", layerMetas->size());
        return layerMeta;
    }

    for (auto &rangeMeta : (*layerMetas)[0]) {
        rangeMeta.quota =  rangeMeta.end - rangeMeta.begin + 1;
    }
    layerMeta.reset(new LayerMeta((*layerMetas)[0]));
    return layerMeta;
}

REGISTER_KERNEL_BUILDER(Name("Ha3AuxScanOp").Device(tensorflow::DEVICE_CPU),
                        Ha3AuxScanOp);

END_HA3_NAMESPACE(turing);
