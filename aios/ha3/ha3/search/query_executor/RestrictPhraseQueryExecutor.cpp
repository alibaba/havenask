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
#include "ha3/search/RestrictPhraseQueryExecutor.h"

#include <assert.h>
#include <iosfwd>

#include "autil/CommonMacros.h"

#include "ha3/isearch.h"
#include "ha3/search/AndQueryExecutor.h"
#include "ha3/search/QueryExecutorRestrictor.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, RestrictPhraseQueryExecutor);

RestrictPhraseQueryExecutor::RestrictPhraseQueryExecutor(QueryExecutorRestrictor *restrictor) {
    _restrictor = restrictor;
}

RestrictPhraseQueryExecutor::~RestrictPhraseQueryExecutor() { 
    DELETE_AND_SET_NULL(_restrictor);
}

indexlib::index::ErrorCode RestrictPhraseQueryExecutor::doSeek(docid_t id, docid_t& result) {
    if (unlikely(_hasSubDocExecutor)) {
        return AndQueryExecutor::doSeek(id, result);
    }
    docid_t tmpid = id;
    do {
        auto ec = AndQueryExecutor::doSeek(tmpid, tmpid);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpid == END_DOCID) {
            result = END_DOCID;
            return indexlib::index::ErrorCode::OK;
        }
        if (tmpid == _lastMatchedDocId) {
            result = tmpid;
            return indexlib::index::ErrorCode::OK;            
        }
        docid_t restrictId = _restrictor->meetRestrict(tmpid);
        assert (restrictId >= tmpid);
        if (restrictId == END_DOCID) {
            result = END_DOCID;
            return indexlib::index::ErrorCode::OK;
        } 
        if (restrictId == tmpid) {
            bool isPhraseFreq = false;
            ec = phraseFreq(isPhraseFreq);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (isPhraseFreq) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return indexlib::index::ErrorCode::OK;
            } else {
                tmpid++;
            }
        } else {
            tmpid = restrictId;
        }
    } while(true);
    
    result = END_DOCID;
    return indexlib::index::ErrorCode::OK;
}

void RestrictPhraseQueryExecutor::reset() {
    PhraseQueryExecutor::reset();
    _restrictor->reset();
}

} // namespace search
} // namespace isearch

