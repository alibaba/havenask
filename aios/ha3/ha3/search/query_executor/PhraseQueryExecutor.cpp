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
#include "ha3/search/PhraseQueryExecutor.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <vector>

#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/AndQueryExecutor.h"
#include "ha3/search/MultiQueryExecutor.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"

using namespace std;
using namespace indexlib::index;
namespace isearch {
namespace search {

AUTIL_LOG_SETUP(ha3, PhraseQueryExecutor);
PhraseQueryExecutor::PhraseQueryExecutor()
    : _lastMatchedDocId(INVALID_DOCID)
{
}

PhraseQueryExecutor::~PhraseQueryExecutor() {
}

void PhraseQueryExecutor::addTermQueryExecutors(const vector<TermQueryExecutor*> &termQueryExecutors) {
    _termExecutors = termQueryExecutors;
    vector<QueryExecutor*> queryExecutors(termQueryExecutors.begin(), termQueryExecutors.end());
    AndQueryExecutor::addQueryExecutors(queryExecutors);
}

indexlib::index::ErrorCode PhraseQueryExecutor::doSeek(docid_t id, docid_t& result) {
    if (unlikely(_hasSubDocExecutor)) {
        return AndQueryExecutor::doSeek(id, result);
    }
    docid_t tmpid = id;
    docid_t tmpResult = INVALID_DOCID;
    do {
        auto ec = AndQueryExecutor::doSeek(tmpid, tmpResult);
        IE_RETURN_CODE_IF_ERROR(ec);
        tmpid = tmpResult;
        if (tmpid != END_DOCID) {
            if (_lastMatchedDocId) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;
            }
            bool isPhraseFreq = false;
            ec = phraseFreq(isPhraseFreq);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (isPhraseFreq) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;
            }
            tmpid++;
        } else {
            result = END_DOCID;
            return indexlib::index::ErrorCode::OK;            
        }
    } while(true);

    result = END_DOCID;
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::ErrorCode PhraseQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId, 
                           docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    if (likely(!_hasSubDocExecutor)) {
        return AndQueryExecutor::seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata, result);
    }
    docid_t tmpid = subDocId;
    docid_t tmpResult = INVALID_DOCID;
    do {
        auto ec = AndQueryExecutor::seekSubDoc(docId, tmpid, subDocEnd, needSubMatchdata, tmpResult);
        IE_RETURN_CODE_IF_ERROR(ec);
        tmpid = tmpResult;
        if (tmpid != END_DOCID) {
            if (_lastMatchedDocId == tmpid) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;
            }
            bool isPhraseFreq = false;
            ec = phraseFreq(isPhraseFreq);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (isPhraseFreq) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;                
            }
            tmpid++;
        } else {
            result = END_DOCID;
            return indexlib::index::ErrorCode::OK;
        }
    } while(true);
    
    result = END_DOCID;
    return indexlib::index::ErrorCode::OK;
}

void PhraseQueryExecutor::addRelativePostion(int32_t termPos, int32_t postingMark)
{
    _termReleativePos.push_back(termPos);
    _postingsMark.push_back(postingMark);
}


string PhraseQueryExecutor::toString() const {
    return "PHRASE" + MultiQueryExecutor::toString();
}


} // namespace search
} // namespace isearch
