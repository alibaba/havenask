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

#include "indexlib/base/Types.h"
#include "sql/ops/join/LookupR.h"
#include "sql/ops/scan/NormalScanR.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/UseSubR.h"
#include "suez/turing/navi/QueryMemPoolR.h"

namespace isearch {
namespace search {
class MatchDataCollectMatchedInfo;
} // namespace search
namespace common {
class Query;
class ColumnTerm;
} // namespace common
} // namespace isearch

namespace sql {

class LookupNormalR : public LookupR {
public:
    LookupNormalR();
    ~LookupNormalR();
    LookupNormalR(const LookupNormalR &) = delete;
    LookupNormalR &operator=(const LookupNormalR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    std::shared_ptr<StreamQuery> genFinalStreamQuery(const LookupJoinBatch &batch) override;

private:
    bool doJoinTable(const LookupJoinBatch &batch, const table::TablePtr &streamOutput) override;

private:
    void patchLookupHintInfo();
    void createMatchedRowIndexs(const table::TablePtr &streamOutput);
    bool docIdsJoin(const LookupJoinBatch &batch, const table::TablePtr &streamOutput);
    std::shared_ptr<isearch::common::Query> genTableQuery(const LookupJoinBatch &batch);
    isearch::common::ColumnTerm *makeColumnTerm(const LookupJoinBatch &batch,
                                                const std::string &inputField,
                                                const std::string &joinField,
                                                bool &returnEmptyQuery);
    bool genRowGroupKey(const LookupJoinBatch &batch,
                        const std::vector<table::Column *> &singleTermCols,
                        std::vector<std::string> &rowGroupKey);
    bool genSingleTermColumns(const table::TablePtr &input,
                              const std::vector<table::Column *> &singleTermCols,
                              const std::vector<uint16_t> &singleTermID,
                              const std::unordered_map<std::string, std::vector<size_t>> &rowGroups,
                              size_t rowCount,
                              std::vector<isearch::common::ColumnTerm *> &terms);
    bool genMultiTermColumns(const table::TablePtr &input,
                             const std::vector<table::Column *> &MultiTermCols,
                             const std::vector<uint16_t> &multiTermID,
                             const std::unordered_map<std::string, std::vector<size_t>> &rowGroups,
                             size_t rowCount,
                             std::vector<isearch::common::ColumnTerm *> &terms);
    void fillTerms(const LookupJoinBatch &batch, std::vector<isearch::common::ColumnTerm *> &terms);
    void fillTermsWithRowOptimized(const LookupJoinBatch &batch,
                                   std::vector<isearch::common::ColumnTerm *> &terms);

private:
    static bool selectValidField(const table::TablePtr &input,
                                 std::vector<std::string> &inputFields,
                                 std::vector<std::string> &joinFields,
                                 bool enableRowDeduplicate);
    static bool genDocIds(const LookupJoinBatch &batch,
                          const std::string &field,
                          std::vector<indexlib::docid_t> &docIds);
    static bool getDocIdField(const std::vector<std::string> &inputFields,
                              const std::vector<std::string> &joinFields,
                              std::string &field);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE_BASE(LookupR);

public:
    RESOURCE_DEPEND_ON(suez::turing::QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(NormalScanR, _normalScanR);
    RESOURCE_DEPEND_ON(UseSubR, _useSubR);
    RESOURCE_DEPEND_ON(ScanInitParamR, _scanInitParamR);
    std::set<std::string> _disableCacheFields;
    std::unique_ptr<isearch::search::MatchDataCollectMatchedInfo> _matchedInfo;
    std::vector<std::vector<size_t>> _rowMatchedInfo;
    bool _useMatchedRow = true;
    bool _enableRowDeduplicate = false;
    bool _disableMatchRowIndexOptimize = false;
    bool _isDocIdsOptimize = false;
};

} // namespace sql
