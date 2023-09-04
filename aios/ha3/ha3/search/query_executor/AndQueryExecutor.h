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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MultiQueryExecutor.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/DocValueFilter.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {

class AndQueryExecutor : public MultiQueryExecutor {
public:
    AndQueryExecutor();
    virtual ~AndQueryExecutor();

public:
    const std::string getName() const override;
    df_t getDF(GetDFType type) const override;
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;
    bool isMainDocHit(docid_t docId) const override;
    void addQueryExecutors(const std::vector<QueryExecutor *> &queryExecutor) override;

public:
    std::string toString() const override;
    void accept(ExecutorVisitor *visitor) const override;
    uint32_t getSeekDocCount() override {
        return MultiQueryExecutor::getSeekDocCount() + _testDocCount;
    }

private:
    bool testCurrentDoc(docid_t docid);

protected:
    QueryExecutorVector _sortedQueryExecutors;
    std::vector<indexlib::index::DocValueFilter *> _filters;
    QueryExecutor **_firstExecutor;
    int64_t _testDocCount;

private:
    AUTIL_LOG_DECLARE();
};

inline bool AndQueryExecutor::testCurrentDoc(docid_t docid) {
    size_t filterSize = _filters.size();
    for (size_t i = 0; i < filterSize; i++) {
        _testDocCount++;
        if (!_filters[i]->Test(docid)) {
            return false;
        }
    }
    return true;
}

} // namespace search
} // namespace isearch
