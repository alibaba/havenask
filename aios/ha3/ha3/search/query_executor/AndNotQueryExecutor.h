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

#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MultiQueryExecutor.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class DocValueFilter;
} // namespace index
} // namespace indexlib

namespace isearch {
namespace search {
class ExecutorVisitor;

class AndNotQueryExecutor : public MultiQueryExecutor {
public:
    AndNotQueryExecutor();
    ~AndNotQueryExecutor();

public:
    const std::string getName() const override {
        return "AndNotQueryExecutor";
    }
    void accept(ExecutorVisitor *visitor) const override;
    df_t getDF(GetDFType type) const override;
    void addQueryExecutors(const std::vector<QueryExecutor *> &queryExecutors) override {
        assert(false);
    }
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;
    bool isMainDocHit(docid_t docId) const override;
    void setCurrSub(docid_t docid) override;
    uint32_t getSeekDocCount() override {
        return MultiQueryExecutor::getSeekDocCount() + _testDocCount;
    }

public:
    void addQueryExecutors(QueryExecutor *leftExecutor, QueryExecutor *rightExecutor);

public:
    std::string toString() const override;

private:
    indexlib::index::ErrorCode doSeek(docid_t docId, docid_t &result) override;

private:
    QueryExecutor *_leftQueryExecutor;
    QueryExecutor *_rightQueryExecutor;
    indexlib::index::DocValueFilter *_rightFilter;
    int64_t _testDocCount;
    bool _rightExecutorHasSub;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
