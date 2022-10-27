#include <ha3/turing/ops/agg/SeekIteratorPrepareOp.h>
#include <ha3/common/Request.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/PKQueryExecutor.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <indexlib/misc/exception.h>
#include <indexlib/partition/index_partition_reader.h>
#include <ha3/turing/variant/LayerMetasVariant.h>

using namespace std;
using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SeekIteratorPrepareOp);

void SeekIteratorPrepareOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    auto layerMetasVariant = ctx->input(0).scalar<Variant>()().get<LayerMetasVariant>();
    OP_REQUIRES(ctx, layerMetasVariant, errors::Unavailable("get layer metas variant failed"));

    LayerMetasPtr layerMetas = layerMetasVariant->getLayerMetas();

    auto resourceVariant = ctx->input(1).scalar<Variant>()().get<ExpressionResourceVariant>();
    OP_REQUIRES(ctx, resourceVariant, errors::Unavailable("get expression resource variant failed"));
    ExpressionResourcePtr resource = resourceVariant->getExpressionResource();
    OP_REQUIRES(ctx, resource, errors::Unavailable("get expression resource is empty"));

    string mainTableName = sessionResource->bizInfo._itemTableName;
    SeekIteratorPtr seekIterator;
    bool ret = createSeekIterator(mainTableName, resource, layerMetas,
                                  queryResource, seekIterator);
    OP_REQUIRES(ctx, ret, errors::Unavailable("create seek iterator failed."));
    // output
    SeekIteratorVariant seekIteratorVariant(seekIterator, layerMetas, resource);
    Tensor *outputTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
    outputTensor->scalar<Variant>()() = seekIteratorVariant;
}

bool SeekIteratorPrepareOp::createSeekIterator(const string &mainTableName,
        const ExpressionResourcePtr &resource,
        const LayerMetasPtr &layerMetas,
        QueryResource *queryResource,
        SeekIteratorPtr &seekIterator)
{
    if (layerMetas == NULL || layerMetas->size() == 0) {
        return true;
    }

    autil::mem_pool::Pool *pool = queryResource->getPool();
    RequestPtr request = resource->_request;
    FilterClause *filterClause = request->getFilterClause();
    FilterWrapperPtr filterWrapper;
    bool ret = createFilterWrapper(filterClause,
                                   resource->_attributeExpressionCreator.get(),
                                   resource->_matchDocAllocator.get(), pool,
                                   filterWrapper);
    if (!ret) {
        HA3_LOG(WARN, "create filter wrapper failed");
        return false;
    }

    ConfigClause *configClause = request->getConfigClause();
    if (!configClause) {
        HA3_LOG(WARN, "config clause unavailable");
        return false;
    }
    bool ignoreDelete = configClause->ignoreDelete() == ConfigClause::CB_TRUE;

    auto partitionReaderSnapshot = queryResource->getIndexSnapshot();
    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(partitionReaderSnapshot, mainTableName);
    if (indexPartitionReaderWrapper == NULL) {
        HA3_LOG(WARN, "create index partition reader wrapper failed.");
        return false;
    }
    indexPartitionReaderWrapper->setSessionPool(pool);
    auto timeoutTerminator = queryResource->getTimeoutTerminator();
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr subDelMapReader;
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *mainToSubIt = NULL;
    auto indexPartitionReader = indexPartitionReaderWrapper->getReader();
    if (!ignoreDelete) {
        delMapReader = indexPartitionReader->GetDeletionMapReader();
    }
    if (configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO) {
        mainToSubIt = indexPartitionReaderWrapper->getMainToSubIter();
        const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &subIndexPartReader =
            indexPartitionReaderWrapper->getSubReader();
        if (subIndexPartReader && !ignoreDelete) {
            subDelMapReader = subIndexPartReader->GetDeletionMapReader();
        }
    }
    LayerMeta &layerMeta = (*layerMetas)[0];
    PKFilterClause *pkFilterClause = request->getPKFilterClause();
    QueryClause *queryClause = request->getQueryClause();
    if (!queryClause) {
        HA3_LOG(WARN, "create query clause failed");
        return false;
    }
    QueryExecutor *queryExecutor = createQueryExecutor(queryClause->getRootQuery(),
            indexPartitionReaderWrapper.get(), pkFilterClause,
            &layerMeta, pool, timeoutTerminator,
            indexPartitionReader);
    if (!queryExecutor) {
        HA3_LOG(DEBUG, "create query executor failed."); // query term not exist
        return true;
    }
    QueryExecutorPtr queryExecutorPtr(queryExecutor, [](QueryExecutor *p) {
                POOL_DELETE_CLASS(p);
            });
    bool needAllSubDoc = request->getConfigClause()->getAllSubDoc();
    SeekIteratorParam seekParam;
    seekParam.indexPartitionReaderWrapper = indexPartitionReaderWrapper;
    seekParam.queryExecutor = queryExecutorPtr;
    seekParam.filterWrapper = filterWrapper;
    seekParam.matchDocAllocator = resource->_matchDocAllocator;
    seekParam.delMapReader = delMapReader;
    seekParam.subDelMapReader = subDelMapReader;
    seekParam.layerMeta = &layerMeta;
    seekParam.mainToSubIt = mainToSubIt;
    seekParam.timeoutTerminator = timeoutTerminator;
    seekParam.needAllSubDocFlag = needAllSubDoc;
    seekIterator.reset(new SeekIterator(seekParam));
    return true;
}

bool SeekIteratorPrepareOp::createFilterWrapper(const FilterClause *filterClause,
        AttributeExpressionCreator *attrExprCreator,
        MatchDocAllocator *matchDocAllocator,
        autil::mem_pool::Pool *pool, FilterWrapperPtr &filterWrapper)
{
    Filter *filter = NULL;
    SubDocFilter *subDocFilter = NULL;
    if (filterClause) {
        filter = Filter::createFilter(filterClause, attrExprCreator, pool);
        if (filter == NULL) {
            HA3_LOG(WARN, "Create Filter failed.");
            return false;
        }
    }
    if (matchDocAllocator->hasSubDocAllocator()) {
        subDocFilter = POOL_NEW_CLASS(pool, SubDocFilter,
                matchDocAllocator->getSubDocAccessor());
    }
    if (filter != NULL || subDocFilter != NULL) {
        filterWrapper.reset(new FilterWrapper());
        filterWrapper->setFilter(filter);
        filterWrapper->setSubDocFilter(subDocFilter);
    }
    return true;
}

QueryExecutor* SeekIteratorPrepareOp::createQueryExecutor(const Query *query,
        IndexPartitionReaderWrapper *wrapper,
        const PKFilterClause *pkFilterClause,
        const LayerMeta *layerMeta,
        autil::mem_pool::Pool *pool,
        TimeoutTerminator *timeoutTerminator,
        const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &indexPartReaderPtr)
{
    QueryExecutor *queryExecutor = NULL;
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;
    try {
        HA3_LOG(DEBUG, "query:[%p]", query);
        // no match data manager
        QueryExecutorCreator qeCreator(NULL, wrapper, pool, timeoutTerminator, layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
        if (queryExecutor->isEmpty()) {
            POOL_DELETE_CLASS(queryExecutor);
            queryExecutor = NULL;
        }
        if (pkFilterClause) {
            QueryExecutor *pkExecutor = createPKExecutor(
                    pkFilterClause, queryExecutor, indexPartReaderPtr, pool);
            queryExecutor = pkExecutor;
        }
    } catch (const IE_NAMESPACE(misc)::ExceptionBase &e) {
        HA3_LOG(WARN, "lookup exception: %s", e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception& e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_LOOKUP;
    }
    if (errorCode != ERROR_NONE) {
        HA3_LOG(WARN, "Exception: [%s]", errorMsg.c_str());
        HA3_LOG(WARN, "Create query executor failed, query [%s]",
                query->toString().c_str());
    }
    return queryExecutor;
}

QueryExecutor* SeekIteratorPrepareOp::createPKExecutor(
        const PKFilterClause *pkFilterClause, QueryExecutor *queryExecutor,
        const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &indexPartReaderPtr,
        autil::mem_pool::Pool *pool) {
    assert(pkFilterClause);
    const IE_NAMESPACE(index)::IndexReaderPtr &primaryKeyReaderPtr
        = indexPartReaderPtr->GetPrimaryKeyReader();
    if (!primaryKeyReaderPtr) {
        HA3_LOG(WARN, "pkFilterClause is NULL");
        return NULL;
    }
    IE_NAMESPACE(util)::Term term(pkFilterClause->getOriginalString(), "");
    IE_NAMESPACE(index)::PostingIterator *iter = primaryKeyReaderPtr->Lookup(term, 1,
            pt_default, pool);
    docid_t docId = iter ? iter->SeekDoc(0) : END_DOCID;
    QueryExecutor *pkExecutor = POOL_NEW_CLASS(pool, PKQueryExecutor, queryExecutor, docId);
    POOL_DELETE_CLASS(iter);
    return pkExecutor;
}



REGISTER_KERNEL_BUILDER(Name("SeekIteratorPrepareOp")
                        .Device(DEVICE_CPU),
                        SeekIteratorPrepareOp);

END_HA3_NAMESPACE(turing);
