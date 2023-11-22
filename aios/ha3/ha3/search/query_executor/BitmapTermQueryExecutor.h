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

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace common {
class Term;
} // namespace common
} // namespace isearch

namespace isearch {
namespace search {

class BitmapTermQueryExecutor : public TermQueryExecutor {
public:
    BitmapTermQueryExecutor(indexlib::index::PostingIterator *iter, const common::Term &term);
    ~BitmapTermQueryExecutor();

private:
    BitmapTermQueryExecutor(const BitmapTermQueryExecutor &);
    BitmapTermQueryExecutor &operator=(const BitmapTermQueryExecutor &);

public:
    void reset() override;
    const std::string getName() const override {
        return "BitmapTermQueryExecutor";
    }

public:
    inline bool test(docid_t docId) {
        if (_bitmapIter->Test(docId)) {
            setDocId(docId);
            return true;
        }
        return false;
    }

private:
    void initBitmapIterator() {
        _bitmapIter = dynamic_cast<indexlib::index::BitmapPostingIterator *>(_iter);
    }
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override {
        indexlib::docid64_t tempDocId = INVALID_DOCID;
        auto ec = _bitmapIter->InnerSeekDoc(id, tempDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        ++_seekDocCount;
        if (tempDocId == INVALID_DOCID) {
            tempDocId = END_DOCID;
        }
        result = tempDocId;
        return indexlib::index::ErrorCode::OK;
    }

private:
    indexlib::index::BitmapPostingIterator *_bitmapIter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BitmapTermQueryExecutor> BitmapTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
