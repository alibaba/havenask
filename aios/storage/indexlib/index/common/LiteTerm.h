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

#include "indexlib/base/Types.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/Types.h"

namespace indexlib::index {

class LiteTerm
{
public:
    LiteTerm() : _indexId(INVALID_INDEXID), _truncateIndexId(INVALID_INDEXID), _termHashKey(INVALID_DICT_VALUE) {}

    LiteTerm(dictkey_t termHashKey, indexid_t indexId, indexid_t truncIndexId = INVALID_INDEXID)
        : _indexId(indexId)
        , _truncateIndexId(truncIndexId)
        , _termHashKey(termHashKey)
    {
    }

    ~LiteTerm() {}

public:
    dictkey_t GetTermHashKey() const { return _termHashKey; }
    indexid_t GetIndexId() const { return _indexId; }
    indexid_t GetTruncateIndexId() const { return _truncateIndexId; }

    void SetTermHashKey(dictkey_t termHashKey) { _termHashKey = termHashKey; }
    void SetIndexId(indexid_t indexId) { _indexId = indexId; }
    void SetTruncateIndexId(indexid_t truncIndexId) { _truncateIndexId = truncIndexId; }

    bool operator==(const LiteTerm& term) const
    {
        return _indexId == term._indexId && _truncateIndexId == term._truncateIndexId &&
               _termHashKey == term._termHashKey;
    }

    bool operator!=(const LiteTerm& term) const { return !(*this == term); }

private:
    indexid_t _indexId;
    indexid_t _truncateIndexId;
    dictkey_t _termHashKey;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LiteTerm> LiteTermPtr;
} // namespace indexlib::index
