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
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {

class DocIdsQueryExecutor : public QueryExecutor {
public:
    DocIdsQueryExecutor(const std::vector<docid_t> &docIds);
    ~DocIdsQueryExecutor();

public:
    const std::string getName() const override;
    void reset() override;
    void moveToEnd() override;
    uint32_t getSeekDocCount() override;
    std::string toString() const override;
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;
    bool isMainDocHit(docid_t docId) const override;
    df_t getDF(GetDFType type) const override;

protected:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;

private:
    std::vector<docid_t> _docIds;
    size_t _curPos;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
