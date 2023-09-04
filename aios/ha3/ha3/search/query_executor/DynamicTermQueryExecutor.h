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
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class DynamicPostingIterator;
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

class DynamicTermQueryExecutor : public TermQueryExecutor {
public:
    DynamicTermQueryExecutor(indexlib::index::PostingIterator *iter, const common::Term &term);
    ~DynamicTermQueryExecutor();

private:
    DynamicTermQueryExecutor(const DynamicTermQueryExecutor &);
    DynamicTermQueryExecutor &operator=(const DynamicTermQueryExecutor &);

public:
    const std::string getName() const override {
        return "DynamicTermQueryExecutor";
    }
    void reset() override {
        TermQueryExecutor::reset();
        initDynamicPostingIterator();
    }

private:
    void initDynamicPostingIterator();

protected:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;

protected:
    indexlib::index::DynamicPostingIterator *_dynamicIter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DynamicTermQueryExecutor> DynamicTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
