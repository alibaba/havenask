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
#include "ha3/search/BitmapAndQueryExecutor.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <limits>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"

#include "ha3/isearch.h"
#include "ha3/search/AndQueryExecutor.h"
#include "ha3/search/BitmapTermQueryExecutor.h"
#include "ha3/search/ExecutorVisitor.h"
#include "ha3/search/MultiQueryExecutor.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, BitmapAndQueryExecutor);

BitmapAndQueryExecutor::BitmapAndQueryExecutor(autil::mem_pool::Pool *pool) {
    _pool = pool;
    _seekQueryExecutor = NULL;
    _firstExecutor = NULL;
}

BitmapAndQueryExecutor::~BitmapAndQueryExecutor() { 
}

void BitmapAndQueryExecutor::accept(ExecutorVisitor *visitor) const {
    visitor->visitBitmapAndExecutor(this);
}

void BitmapAndQueryExecutor::addQueryExecutors(const vector<QueryExecutor*> &queryExecutors) {
    assert(queryExecutors.size() >= 2);
    vector<QueryExecutor*> seekQueryExecutors;
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        QueryExecutor *queryExecutor = queryExecutors[i];
        if (queryExecutor->getName() == "BitmapTermQueryExecutor") {
            BitmapTermQueryExecutor *bitmapTermExecutor =
                dynamic_cast<BitmapTermQueryExecutor*>(queryExecutor);
            assert(bitmapTermExecutor);
            _bitmapTermExecutors.push_back(bitmapTermExecutor);
        } else {
            seekQueryExecutors.push_back(queryExecutor);
        }
    }
    sort(_bitmapTermExecutors.begin(), _bitmapTermExecutors.end(), DFCompare());
    if (seekQueryExecutors.empty()) {
        seekQueryExecutors.push_back(_bitmapTermExecutors[0]);
        _bitmapTermExecutors.erase(_bitmapTermExecutors.begin());
    }
    if (seekQueryExecutors.size() == 1) {
        _seekQueryExecutor = seekQueryExecutors[0];
    } else {
        AndQueryExecutor *andQueryExecutor = POOL_NEW_CLASS(_pool,
                AndQueryExecutor);
        andQueryExecutor->addQueryExecutors(seekQueryExecutors);
        _seekQueryExecutor = andQueryExecutor;
    }
    _hasSubDocExecutor = _seekQueryExecutor->hasSubDocExecutor();
    for (size_t i = 0; i < _bitmapTermExecutors.size(); i++) {
        _hasSubDocExecutor = _hasSubDocExecutor || _bitmapTermExecutors[i]->hasSubDocExecutor();
    }
    _firstExecutor = &_bitmapTermExecutors[0];
    // add all query executor to multi query executor for deconstruct,unpack,getMetaInfo.
    _queryExecutors.push_back(_seekQueryExecutor);
    _queryExecutors.insert(_queryExecutors.end(), _bitmapTermExecutors.begin(), _bitmapTermExecutors.end());
}

indexlib::index::ErrorCode BitmapAndQueryExecutor::doSeek(docid_t docId, docid_t& result) {
    BitmapTermQueryExecutor **firstExecutor = _firstExecutor;
    BitmapTermQueryExecutor **endExecutor = firstExecutor + _bitmapTermExecutors.size();
    while (true) {
        auto ec = _seekQueryExecutor->seek(docId, docId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (unlikely(docId == END_DOCID)) {
            moveToEnd();
            break;
        }
        BitmapTermQueryExecutor **curExecutor = firstExecutor;
        do {
            if (!(*curExecutor)->test(docId)) {
                break;
            }
        } while (++curExecutor < endExecutor);
        if (curExecutor == endExecutor) {
            // valid docId
            break;
        }
        ++docId;
    }
    result = docId;
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::ErrorCode BitmapAndQueryExecutor::seekSubDoc(
        docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    QueryExecutor **firstExecutor = &_queryExecutors[0];
    QueryExecutor **currentExecutor = firstExecutor;
    QueryExecutor **endExecutor = firstExecutor + _queryExecutors.size();
    docid_t current = subDocId;
    do {
        docid_t tmpid = INVALID_DOCID;
        auto ec = (*currentExecutor)->seekSub(docId, current, subDocEnd, needSubMatchdata, tmpid);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpid != current) {
            current = tmpid;
            currentExecutor = firstExecutor;
        } else {
            currentExecutor++;
        }
    } while (current < subDocEnd && currentExecutor < endExecutor);

    result = current;
    return indexlib::index::ErrorCode::OK;
}

bool BitmapAndQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_seekQueryExecutor->isMainDocHit(docId)) {
        return false;
    }
    for (size_t i = 0; i < _bitmapTermExecutors.size(); i++) {
        if(!_bitmapTermExecutors[i]->isMainDocHit(docId)){
            return false;
        }
    }
    return true;
}

df_t BitmapAndQueryExecutor::getDF(GetDFType type) const {
    df_t minDF = numeric_limits<df_t>::max();
    for (size_t i = 0; i < _queryExecutors.size(); ++i) {
        minDF = min(minDF, _queryExecutors[i]->getDF(type));
    }
    return minDF;
}

std::string BitmapAndQueryExecutor::toString() const {
    return "BITMAPAND(" + _seekQueryExecutor->toString()
        + ",TEST" + MultiQueryExecutor::convertToString(_bitmapTermExecutors) + ")";
}

} // namespace search
} // namespace isearch

