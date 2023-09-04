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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MultiQueryExecutor.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/QueryExecutorHeap.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {

class WeakAndQueryExecutor : public MultiQueryExecutor {
public:
    WeakAndQueryExecutor(uint32_t minShouldMatch);
    ~WeakAndQueryExecutor();

public:
    const std::string getName() const override {
        return "WeakAndQueryExecutor";
    }
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;
    df_t getDF(GetDFType type) const override;
    bool isMainDocHit(docid_t docId) const override;
    void addQueryExecutors(const std::vector<QueryExecutor *> &queryExecutors) override;
    void reset() override;
    std::string toString() const override;

private:
    void doNthElement(std::vector<QueryExecutorEntry> &heap);

private:
    std::vector<QueryExecutorEntry> _sortNHeap;
    std::vector<QueryExecutorEntry> _sortNHeapSub;
    uint32_t _count;
    uint32_t _minShouldMatch;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<WeakAndQueryExecutor> WeakAndQueryExecutorPtr;

} // namespace search
} // namespace isearch
