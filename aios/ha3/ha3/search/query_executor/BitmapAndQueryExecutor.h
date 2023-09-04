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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MultiQueryExecutor.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace search {
class BitmapTermQueryExecutor;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {
class ExecutorVisitor;

class BitmapAndQueryExecutor : public MultiQueryExecutor {
public:
    BitmapAndQueryExecutor(autil::mem_pool::Pool *pool);
    ~BitmapAndQueryExecutor();

private:
    BitmapAndQueryExecutor(const BitmapAndQueryExecutor &);
    BitmapAndQueryExecutor &operator=(const BitmapAndQueryExecutor &);

public:
    void addQueryExecutors(const std::vector<QueryExecutor *> &queryExecutors) override;
    const std::string getName() const override {
        return "BitmapAndQueryExecutor";
    }
    void accept(ExecutorVisitor *visitor) const override;
    df_t getDF(GetDFType type) const override;
    bool isMainDocHit(docid_t docId) const override;

    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;

public:
    std::string toString() const override;

private:
    indexlib::index::ErrorCode doSeek(docid_t docId, docid_t &result) override;

private:
    autil::mem_pool::Pool *_pool;
    QueryExecutor *_seekQueryExecutor;
    std::vector<BitmapTermQueryExecutor *> _bitmapTermExecutors;
    BitmapTermQueryExecutor **_firstExecutor;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BitmapAndQueryExecutor> BitmapAndQueryExecutorPtr;

} // namespace search
} // namespace isearch
