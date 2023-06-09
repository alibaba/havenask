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
#include <stddef.h>
#include <string>

#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {
class LayerMeta;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {

class RangeQueryExecutor : public search::QueryExecutor {
public:
    RangeQueryExecutor(search::LayerMeta *layerMeta);
    ~RangeQueryExecutor();

private:
    RangeQueryExecutor(const RangeQueryExecutor &);
    RangeQueryExecutor &operator=(const RangeQueryExecutor &);

public:
    const std::string getName() const override {
        return "RangeQueryExecutor";
    }
    df_t getDF(GetDFType type) const override {
        return _df;
    }
    void reset() override;
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                               docid_t subDocId,
                                               docid_t subDocEnd,
                                               bool needSubMatchdata,
                                               docid_t &result) override;
    bool isMainDocHit(docid_t docId) const override;

public:
    std::string toString() const override;

private:
    virtual indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;

private:
    search::LayerMeta &_layerMeta;
    size_t _rangeIdx;
    size_t _rangeCount;
    df_t _df;
};

typedef std::shared_ptr<RangeQueryExecutor> RangeQueryExecutorPtr;
} // namespace search
} // namespace isearch
