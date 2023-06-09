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
#include "ha3/search/PKQueryExecutor.h"

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/PoolBase.h"

#include "ha3/isearch.h"
#include "ha3/search/QueryExecutor.h"
#include "autil/Log.h"

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, PKQueryExecutor);

PKQueryExecutor::PKQueryExecutor(QueryExecutor *queryExecutor,
                                 docid_t docId)
{
    _queryExecutor = queryExecutor;
    if (_queryExecutor) {
        _hasSubDocExecutor = _queryExecutor->hasSubDocExecutor();
    }
    _docId = docId;
    _count = 0;
}

PKQueryExecutor::~PKQueryExecutor() {
    POOL_DELETE_CLASS(_queryExecutor);
}

indexlib::index::ErrorCode PKQueryExecutor::doSeek(docid_t id, docid_t& result)
{
    if (!_queryExecutor) {
        AUTIL_LOG(DEBUG,"_indexReader or _queryExecutor is NULL");
        result = END_DOCID;
        return IE_OK;
    }

    if (id > _docId) {
        result = END_DOCID;
        return IE_OK;
    }

    if (_count > 0) {
        result = END_DOCID;
        return IE_OK;
    }
    if (_docId == END_DOCID) {
        result = _docId;
        return IE_OK;
    }
    ++_seekDocCount;
    docid_t tmpid = INVALID_DOCID;
    auto ec = _queryExecutor->seek(_docId, tmpid);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (_docId != tmpid) {
        result = END_DOCID;
        return IE_OK;
    }
    _count++;
    result = _docId;
    return IE_OK;
}

void PKQueryExecutor::reset() {
    _count = 0;
}

std::string PKQueryExecutor::toString() const {
    return autil::StringUtil::toString(_docId);
}

bool PKQueryExecutor::isMainDocHit(docid_t docId) const {
    return _queryExecutor->isMainDocHit(docId);
}

indexlib::index::ErrorCode PKQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    if (getDocId() == docId && subDocId < subDocEnd) {
        result = subDocId;
        return IE_OK;
    } else {
        result = END_DOCID;
        return IE_OK;
    }
}

} // namespace search
} // namespace isearch
