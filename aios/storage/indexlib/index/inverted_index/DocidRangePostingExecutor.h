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
#include <memory>

#include "indexlib/index/inverted_index/PostingExecutor.h"

namespace indexlib::index {

class DocidRangePostingExecutor : public PostingExecutor
{
public:
    // [beginDocId, endDocId)
    DocidRangePostingExecutor(docid_t beginDocId, docid_t endDocId)
        : _beginDocId(beginDocId)
        , _endDocID(endDocId)
        , _currentDocId(beginDocId)
    {
        if (_endDocID <= _beginDocId) {
            _currentDocId = INVALID_DOCID;
        }
    }

    ~DocidRangePostingExecutor() {}

public:
    df_t GetDF() const override { return _endDocID - _beginDocId; }

    docid_t DoSeek(docid_t id) override
    {
        if (id >= _endDocID || _currentDocId == INVALID_DOCID) {
            return END_DOCID;
        }
        if (id > _currentDocId) {
            _currentDocId = id;
        }
        return _currentDocId;
    }

private:
    docid_t _beginDocId = INVALID_DOCID;
    docid_t _endDocID = INVALID_DOCID;
    docid_t _currentDocId = INVALID_DOCID;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
