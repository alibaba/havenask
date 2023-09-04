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
#include "ha3/search/FieldMapTermQueryExecutor.h"
#include "ha3/search/SubTermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class BufferedPostingIterator;
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

class SubFieldMapTermQueryExecutor : public SubTermQueryExecutor {
public:
    SubFieldMapTermQueryExecutor(indexlib::index::PostingIterator *iter,
                                 const common::Term &term,
                                 DocMapAttrIterator *mainToSubIter,
                                 DocMapAttrIterator *subToMainIter,
                                 fieldmap_t fieldMap,
                                 FieldMatchOperatorType opteratorType);

    ~SubFieldMapTermQueryExecutor();

private:
    SubFieldMapTermQueryExecutor(const SubFieldMapTermQueryExecutor &);
    SubFieldMapTermQueryExecutor &operator=(const SubFieldMapTermQueryExecutor &);

public:
    const std::string getName() const override {
        return "SubFieldMapTermQueryExecutor";
    }
    std::string toString() const override;
    void reset() override {
        SubTermQueryExecutor::reset();
        initBufferedPostingIterator();
    }

private:
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;

private:
    void initBufferedPostingIterator();

private:
    indexlib::index::BufferedPostingIterator *_bufferedIter;
    fieldmap_t _fieldMap;
    FieldMatchOperatorType _opteratorType;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SubFieldMapTermQueryExecutor> SubFieldMapTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
