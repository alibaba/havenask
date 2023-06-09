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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

#include "ha3/search/AndQueryExecutor.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/TermQueryExecutor.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {
class PhraseQueryExecutor : public AndQueryExecutor 
{
public:    
    PhraseQueryExecutor();
    ~PhraseQueryExecutor() override;

public:
    const std::string getName() const override {
        return "PhraseQueryExecutor";
    }
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t& result) override;
    indexlib::index::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    void addTermQueryExecutors(const std::vector<TermQueryExecutor*> &termQueryExecutors);
    void addRelativePostion(int32_t termPos, int32_t postingMark);
public:
    std::string toString() const override;
protected:
    inline indexlib::index::ErrorCode phraseFreq(bool& result);
protected:
    docid_t _lastMatchedDocId;
private:
    std::vector<TermQueryExecutor*> _termExecutors;
    std::vector<int32_t> _termReleativePos;
    std::vector<int32_t> _postingsMark;
    
private:
    AUTIL_LOG_DECLARE();
};


inline indexlib::index::ErrorCode PhraseQueryExecutor::phraseFreq(bool& result) {
    int size = (int)_termReleativePos.size();
    int index = 0;
    int count = 0;//match count
    pos_t curPos = 0;
    if ( unlikely(0 == size) ) {
        result = true;
        return IE_OK;
    }
    do {
        int32_t relativePos = _termReleativePos[index];
        pos_t tmpocc = INVALID_POSITION;
        auto ec = _termExecutors[_postingsMark[index]]->seekPosition(
            curPos + relativePos, tmpocc);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpocc == INVALID_POSITION) {
            result = false;
            return IE_OK;
        }
        if (tmpocc != curPos + relativePos) {
            curPos = tmpocc - relativePos;
            count = 1;
        } else {
            count ++;
        }
        ++index;
        if ( index == size ) {
            index = 0;
        }
    } while (count < size);

    result = true;
    return IE_OK;
} 

} // namespace search
} // namespace isearch

