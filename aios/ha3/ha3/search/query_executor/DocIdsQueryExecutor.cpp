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
#include "ha3/search/DocIdsQueryExecutor.h"

#include <algorithm>
#include <sstream>

#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/QueryExecutor.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, DocIdsQueryExecutor);

DocIdsQueryExecutor::DocIdsQueryExecutor(const vector<docid_t> &docIds)
    : _docIds(docIds)
{
    sort(_docIds.begin(), _docIds.end());
    _curPos = 0;
    _hasSubDocExecutor = false;
}

DocIdsQueryExecutor::~DocIdsQueryExecutor() {
}

const string DocIdsQueryExecutor::getName() const {
    return "DocIdsQueryExecutor";
}

void DocIdsQueryExecutor::reset() {
    QueryExecutor::reset();
    _curPos = 0;
}

void DocIdsQueryExecutor::moveToEnd() {
    QueryExecutor::moveToEnd();
    _curPos = _docIds.size();
}

uint32_t DocIdsQueryExecutor::getSeekDocCount() {
    return _docIds.size();
}

string DocIdsQueryExecutor::toString() const {
    stringstream ss;
    ss << "DocIds:[";
    for (size_t i = 0; i < _docIds.size(); ++i) {
        ss << (i == 0 ? "" : ", ") << _docIds[i];
    }
    ss << "]";
    return ss.str();
}

indexlib::index::ErrorCode DocIdsQueryExecutor::seekSubDoc(
    docid_t docId,
    docid_t subDocId,
    docid_t subDocEnd,
    bool needSubMatchdata,
    docid_t& result)
{
    if (getDocId() == docId && subDocId < subDocEnd) {
        result = subDocId;
    } else {
        result = END_DOCID;
    }
    return indexlib::index::ErrorCode::OK;
}

bool DocIdsQueryExecutor::isMainDocHit(docid_t docId) const {
    return true;
}

df_t DocIdsQueryExecutor::getDF(GetDFType type) const {
    return 1;
}

indexlib::index::ErrorCode DocIdsQueryExecutor::doSeek(docid_t id, docid_t &result) {
    for (;_curPos < _docIds.size(); ++_curPos) {
        if (_docIds[_curPos] >= id) {
            result = _docIds[_curPos];
            return indexlib::index::ErrorCode::OK;
        }
    }
    result = END_DOCID;
    return indexlib::index::ErrorCode::OK;
}

} // namespace search
} // namespace isearch
