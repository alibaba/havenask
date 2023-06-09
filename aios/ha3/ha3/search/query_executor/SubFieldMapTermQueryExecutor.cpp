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
#include "ha3/search/SubFieldMapTermQueryExecutor.h"

#include <iosfwd>
#include <stdint.h>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/search/FieldMapTermQueryExecutor.h"
#include "ha3/search/SubTermQueryExecutor.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

using namespace std;

using namespace indexlib::index;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SubFieldMapTermQueryExecutor);

SubFieldMapTermQueryExecutor::SubFieldMapTermQueryExecutor(indexlib::index::PostingIterator *iter,
                                                           const common::Term &term,
                                                           DocMapAttrIterator *mainToSubIter,
                                                           DocMapAttrIterator *subToMainIter,
                                                           fieldmap_t fieldMap,
                                                           FieldMatchOperatorType opteratorType)
    : SubTermQueryExecutor(iter, term, mainToSubIter, subToMainIter)
    , _fieldMap(fieldMap)
    , _opteratorType(opteratorType) {
    initBufferedPostingIterator();
}

SubFieldMapTermQueryExecutor::~SubFieldMapTermQueryExecutor() {}

indexlib::index::ErrorCode SubFieldMapTermQueryExecutor::seekSubDoc(
    docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata, docid_t &result) {
    fieldmap_t targetFieldMap = _fieldMap;
    FieldMatchOperatorType opteratorType = _opteratorType;
    while (true) {
        auto ec = SubTermQueryExecutor::seekSubDoc(
            docId, subDocId, subDocEnd, needSubMatchdata, subDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (subDocId == END_DOCID) {
            break;
        }
        fieldmap_t fieldMap;
        ec = _bufferedIter->GetFieldMap(fieldMap);
        IE_RETURN_CODE_IF_ERROR(ec);
        fieldmap_t andResult = fieldMap & targetFieldMap;
        if (opteratorType == FM_AND) {
            if (andResult == targetFieldMap) {
                break;
            }
        } else {
            if (andResult > 0) {
                break;
            }
        }
        ++subDocId;
    }
    result = subDocId;
    return indexlib::index::ErrorCode::OK;
}

std::string SubFieldMapTermQueryExecutor::toString() const {
    std::string ret = _term.getIndexName();
    ret += _opteratorType == FM_OR ? '[' : '(';
    ret += autil::StringUtil::toString((uint32_t)_fieldMap);
    ret += _opteratorType == FM_OR ? ']' : ')';
    ret += ':';
    ret += _term.getWord().c_str();
    return ret;
}

void SubFieldMapTermQueryExecutor::initBufferedPostingIterator() {
    _bufferedIter = dynamic_cast<indexlib::index::BufferedPostingIterator *>(_iter);
}

} // namespace search
} // namespace isearch
