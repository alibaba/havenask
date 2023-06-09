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
#include "ha3/turing/ops/agg/SeekIteratorPrepareOp.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/PKFilterClause.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/PKQueryExecutor.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/QueryExecutorCreator.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "ha3/turing/variant/LayerMetasVariant.h"
#include "ha3/turing/variant/SeekIteratorVariant.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/index/common/Term.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"

namespace indexlib {
namespace index {
class JoinDocidAttributeIterator;
}  // namespace index
}  // namespace indexlib



using namespace std;
using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, SeekIteratorPrepareOp);

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

    string mainTableName = sessionResource->bizInfo->_itemTableName;
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
        AUTIL_LOG(WARN, "create filter wrapper failed");
        return false;
    }

    ConfigClause *configClause = request->getConfigClause();
    if (!configClause) {
        AUTIL_LOG(WARN, "config clause unavailable");
        return false;
    }
    bool ignoreDelete = configClause->ignoreDelete() == ConfigClause::CB_TRUE;

    auto partitionReaderSnapshot = queryResource->getIndexSnapshot();
    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(partitionReaderSnapshot, mainTableName);
    if (indexPartitionReaderWrapper == NULL) {
        AUTIL_LOG(WARN, "create index partition reader wrapper failed.");
        return false;
    }
    indexPartitionReaderWrapper->setSessionPool(pool);
    auto timeoutTerminator = queryResource->getTimeoutTerminator();
    shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    indexlib::index::DeletionMapReaderPtr subDelMapReader;
    indexlib::index::JoinDocidAttributeIterator *mainToSubIt = NULL;
    if (!ignoreDelete) {
        delMapReader = indexPartitionReaderWrapper->getDeletionMapReader();
    }
    if (configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO) {
        mainToSubIt = indexPartitionReaderWrapper->getMainToSubIter();
        const indexlib::partition::IndexPartitionReaderPtr &subIndexPartReader =
            indexPartitionReaderWrapper->getSubReader();
        if (subIndexPartReader && !ignoreDelete) {
            subDelMapReader = subIndexPartReader->GetDeletionMapReader();
        }
    }
    LayerMeta &layerMeta = (*layerMetas)[0];
    PKFilterClause *pkFilterClause = request->getPKFilterClause();
    QueryClause *queryClause = request->getQueryClause();
    if (!queryClause) {
        AUTIL_LOG(WARN, "create query clause failed");
        return false;
    }
    QueryExecutor *queryExecutor = createQueryExecutor(queryClause->getRootQuery(),
            indexPartitionReaderWrapper.get(), pkFilterClause,
            &layerMeta, pool, timeoutTerminator);
    if (!queryExecutor) {
        AUTIL_LOG(DEBUG, "create query executor failed."); // query term not exist
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
        filter = Filter::createFilter(filterClause->getRootSyntaxExpr(), attrExprCreator, pool);
        if (filter == NULL) {
            AUTIL_LOG(WARN, "Create Filter failed.");
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
        autil::TimeoutTerminator *timeoutTerminator)
{
    QueryExecutor *queryExecutor = NULL;
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;
    try {
        AUTIL_LOG(DEBUG, "query:[%p]", query);
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
                    pkFilterClause, queryExecutor, wrapper, pool);
            queryExecutor = pkExecutor;
        }
    } catch (const indexlib::util::ExceptionBase &e) {
        AUTIL_LOG(WARN, "lookup exception: %s", e.what());
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
        AUTIL_LOG(WARN, "Exception: [%s]", errorMsg.c_str());
        AUTIL_LOG(WARN, "Create query executor failed, query [%s]",
                query->toString().c_str());
    }
    return queryExecutor;
}

QueryExecutor* SeekIteratorPrepareOp::createPKExecutor(
        const PKFilterClause *pkFilterClause, QueryExecutor *queryExecutor,
        IndexPartitionReaderWrapper* wrapper,
        autil::mem_pool::Pool *pool) {
    assert(pkFilterClause);
    indexlib::index::InvertedIndexReader *primaryKeyReader
        = wrapper->getPrimaryKeyReader().get();
    if (!primaryKeyReader) {
        AUTIL_LOG(WARN, "pkFilterClause is NULL");
        return NULL;
    }
    indexlib::index::Term term(pkFilterClause->getOriginalString(), "");
    indexlib::index::PostingIterator *iter = primaryKeyReader->Lookup(term, 1,
                                                                      pt_default, pool).ValueOrThrow();
    docid_t docId = iter ? iter->SeekDoc(0) : END_DOCID;
    QueryExecutor *pkExecutor = POOL_NEW_CLASS(pool, PKQueryExecutor, queryExecutor, docId);
    POOL_DELETE_CLASS(iter);
    return pkExecutor;
}



REGISTER_KERNEL_BUILDER(Name("SeekIteratorPrepareOp")
                        .Device(DEVICE_CPU),
                        SeekIteratorPrepareOp);

} // namespace turing
} // namespace isearch
