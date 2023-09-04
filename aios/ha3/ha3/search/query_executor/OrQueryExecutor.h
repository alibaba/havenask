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
class ExecutorVisitor;

class OrQueryExecutor : public MultiQueryExecutor {
public:
    OrQueryExecutor();
    virtual ~OrQueryExecutor();

public:
    const std::string getName() const override {
        return "OrQueryExecutor";
    }
    void accept(ExecutorVisitor *visitor) const override;
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

protected:
    std::vector<QueryExecutorEntry> _qeMinHeap;
    uint32_t _count;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
