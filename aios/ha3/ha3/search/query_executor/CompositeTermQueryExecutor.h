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
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/CompositePostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace common {
class Term;
} // namespace common
} // namespace isearch

namespace isearch {
namespace search {
class CompositeTermQueryExecutor : public TermQueryExecutor {
public:
    CompositeTermQueryExecutor(indexlib::index::PostingIterator *iter, const common::Term &term);
    ~CompositeTermQueryExecutor();

private:
    CompositeTermQueryExecutor(const CompositeTermQueryExecutor &);
    CompositeTermQueryExecutor &operator=(const CompositeTermQueryExecutor &);

public:
    const std::string getName() const override {
        return "CompositeTermQueryExecutor";
    }
    void reset() override {
        TermQueryExecutor::reset();
        initCompositePostingIterator();
    }

private:
    void initCompositePostingIterator();

protected:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;

protected:
    indexlib::index::BufferedCompositePostingIterator *_compositeIter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompositeTermQueryExecutor> CompositeTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
