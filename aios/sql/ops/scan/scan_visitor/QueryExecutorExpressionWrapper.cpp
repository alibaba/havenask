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
#include "sql/ops/scan/QueryExecutorExpressionWrapper.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/Query.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/QueryExecutorCreator.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "matchdoc/MatchDoc.h"
#include "sql/common/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, QueryExecutorExpressionWrapper);

QueryExecutorExpressionWrapper::QueryExecutorExpressionWrapper(isearch::common::Query *query)
    : _query(query)
    , _initialized(false) {}

QueryExecutorExpressionWrapper::~QueryExecutorExpressionWrapper() {
    if (_query) {
        DELETE_AND_SET_NULL(_query);
    }
    for (auto queryExecutor : _queryExecutors) {
        POOL_DELETE_CLASS(queryExecutor);
        queryExecutor = NULL;
    }
    _queryExecutors.clear();
    _docIdRanges.clear();
}

bool QueryExecutorExpressionWrapper::init(
    isearch::search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
    const std::string &mainTableName,
    autil::mem_pool::Pool *pool,
    autil::TimeoutTerminator *timeoutTerminator,
    const std::vector<isearch::search::LayerMetaPtr> &layerMetas) {
    if (_initialized) {
        for (auto *queryExecutor : _queryExecutors) {
            queryExecutor->reset();
        }
        return true;
    }
    _initialized = true;
    // size 1 / size > 1, layermeta has only one range
    for (auto layerMeta : layerMetas) {
        if (layerMeta->empty()) {
            SQL_LOG(ERROR,
                    "unexpected layer meta is empty, query [%s]",
                    _query ? _query->toString().c_str() : "");
            return false;
        }
        _docIdRanges.push_back(make_pair((*layerMeta)[0].begin, (*layerMeta)[0].end));
        isearch::search::QueryExecutor *queryExecutor = createQueryExecutor(
            _query, indexPartitionReader, mainTableName, pool, timeoutTerminator, layerMeta.get());
        if (!queryExecutor) {
            SQL_LOG(ERROR,
                    "table [%s], create query executor failed, query [%s]",
                    mainTableName.c_str(),
                    _query ? _query->toString().c_str() : "");
            return false;
        }
        _queryExecutors.push_back(queryExecutor);
    }
    if (_queryExecutors.empty()) {
        SQL_LOG(ERROR, "unexpected query expression is empty");
        return false;
    }
    return true;
}

bool QueryExecutorExpressionWrapper::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    docid_t docId = matchDoc.getDocId();
    int idx = 0;
    if (_docIdRanges.size() > 1) {
        for (int i = 0; i < _docIdRanges.size(); ++i) {
            if (_docIdRanges[i].first <= docId && docId <= _docIdRanges[i].second) {
                idx = i;
                break;
            }
        }
    }
    docid_t tmpDocId;
    indexlib::index::ErrorCode ec = _queryExecutors[idx]->seek(docId, tmpDocId);
    if (ec != indexlib::index::ErrorCode::OK) {
        return false;
    }
    return tmpDocId == docId;
}

isearch::search::QueryExecutor *QueryExecutorExpressionWrapper::createQueryExecutor(
    const isearch::common::Query *query,
    isearch::search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
    const std::string &mainTableName,
    autil::mem_pool::Pool *pool,
    autil::TimeoutTerminator *timeoutTerminator,
    isearch::search::LayerMeta *layerMeta) {
    if (query == nullptr) {
        SQL_LOG(WARN, "query is null");
        return nullptr;
    }
    isearch::search::QueryExecutor *queryExecutor = nullptr;
    try {
        isearch::search::QueryExecutorCreator qeCreator(
            nullptr, indexPartitionReader.get(), pool, timeoutTerminator, layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
    } catch (const indexlib::util::ExceptionBase &e) {
        SQL_LOG(WARN, "lookup exception: [%s] with create query executor", e.what());
    } catch (const std::exception &e) {
        SQL_LOG(WARN, "exception [%s] with create query executor", e.what());
    } catch (...) { SQL_LOG(WARN, "unknown exception with create query executor"); }
    return queryExecutor;
}

void QueryExecutorExpressionWrapper::reset() {
    for (auto *queryExecutor : _queryExecutors) {
        queryExecutor->reset();
    }
}

} // namespace sql
