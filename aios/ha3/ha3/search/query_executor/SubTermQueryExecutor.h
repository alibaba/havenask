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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/TermMatchData.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class PostingIterator;
} // namespace index
} // namespace indexlib
namespace isearch {
namespace common {
class Term;
} // namespace common
} // namespace isearch

namespace isearch {
namespace search {

class SubTermQueryExecutor : public TermQueryExecutor {
public:
    typedef indexlib::index::JoinDocidAttributeIterator DocMapAttrIterator;

public:
    SubTermQueryExecutor(indexlib::index::PostingIterator *iter,
                         const common::Term &term,
                         DocMapAttrIterator *mainToSubIter,
                         DocMapAttrIterator *subToMainIter);
    ~SubTermQueryExecutor();

private:
    SubTermQueryExecutor(const SubTermQueryExecutor &);
    SubTermQueryExecutor &operator=(const SubTermQueryExecutor &);

public:
    indexlib::index::ErrorCode doSeek(docid_t docId, docid_t &result) override;
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;
    bool isMainDocHit(docid_t docId) const override;
    void reset() override;
    indexlib::index::ErrorCode unpackMatchData(rank::TermMatchData &tmd) override {
        // do nothing
        return IE_OK;
    }

public:
    std::string toString() const override;

private:
    inline indexlib::index::ErrorCode subDocSeek(docid_t subDocId, docid_t &result) {
        if (subDocId > _curSubDocId) {
            auto ec = TermQueryExecutor::doSeek(subDocId, _curSubDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
        result = _curSubDocId;
        return indexlib::index::ErrorCode::OK;
    }

private:
    docid_t _curSubDocId;
    DocMapAttrIterator *_mainToSubIter;
    DocMapAttrIterator *_subToMainIter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SubTermQueryExecutor> SubTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
