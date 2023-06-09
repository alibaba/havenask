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
#include "ha3/search/FieldMapTermQueryExecutor.h"

#include <stdint.h>

#include "autil/StringUtil.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"

#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/search/BufferedTermQueryExecutor.h"
#include "autil/Log.h"

namespace indexlib {
namespace index {
class PostingIterator;
}  // namespace index
}  // namespace indexlib

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FieldMapTermQueryExecutor);

FieldMapTermQueryExecutor::FieldMapTermQueryExecutor(
        indexlib::index::PostingIterator *iter, 
        const common::Term &term, fieldmap_t fieldMap, 
        FieldMatchOperatorType opteratorType)
    : BufferedTermQueryExecutor(iter, term)
    , _fieldMap(fieldMap)
    , _opteratorType(opteratorType)
{
    
}

FieldMapTermQueryExecutor::~FieldMapTermQueryExecutor() { 
}

indexlib::index::ErrorCode FieldMapTermQueryExecutor::doSeek(docid_t id, docid_t& result) {
    fieldmap_t targetFieldMap = _fieldMap;
    FieldMatchOperatorType opteratorType = _opteratorType;
    while (true) {
        auto ec = BufferedTermQueryExecutor::doSeek(id, id);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (id == END_DOCID) {
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
        ++id;
    }
    result = id;
    return indexlib::index::ErrorCode::OK;
}

std::string FieldMapTermQueryExecutor::toString() const {
    std::string ret = _term.getIndexName();
    ret += _opteratorType == FM_OR ? '[' : '(';
    ret += autil::StringUtil::toString((uint32_t)_fieldMap);
    ret += _opteratorType == FM_OR ? ']' : ')';
    ret += ':';
    ret += _term.getWord().c_str();
    return ret;
}

} // namespace search
} // namespace isearch

