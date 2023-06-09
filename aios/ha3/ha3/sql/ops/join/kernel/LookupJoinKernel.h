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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "ha3/common/Query.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/TableMeta.h"
#include "ha3/sql/ops/join/JoinKernelBase.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "indexlib/indexlib.h"
#include "navi/common.h"
#include "navi/engine/KernelConfigContext.h"
#include "table/Row.h"
#include "table/Table.h"

namespace isearch {
namespace common {
class ColumnTerm;
} // namespace common
namespace search {
class MatchDataCollectMatchedInfo;
} // namespace search
} // namespace isearch
namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
class AsyncPipe;
} // namespace navi
namespace table {
class Column;
} // namespace table

namespace isearch {
namespace sql {

struct LookupJoinBatch {
    std::shared_ptr<table::Table> table;
    size_t offset = 0;
    size_t count = 0;
    void reset(const std::shared_ptr<table::Table> &table_);
    bool hasNext() const;
    void next(size_t batchNum);
};

class LookupJoinKernel : public JoinKernelBase {
public:
    LookupJoinKernel();
    ~LookupJoinKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    navi::ErrorCode finishLastJoin(navi::KernelComputeContext &runContext);
    navi::ErrorCode startNewLookup(navi::KernelComputeContext &runContext);
    void createMatchedRowIndexs(const table::TablePtr &streamOutput);
    bool joinTable(const LookupJoinBatch &batch,
                   const table::TablePtr &streamOutput,
                   table::TablePtr &outputTable);
    bool docIdsJoin(const LookupJoinBatch &batch, const table::TablePtr &streamOutput);
    bool doHashJoin(const LookupJoinBatch &batch, const table::TablePtr &streamTable);
    StreamQueryPtr genFinalStreamQuery(const LookupJoinBatch &batch);
    bool genStreamKeys(const LookupJoinBatch &batch, std::vector<std::string> &pks);
    void patchLookupHintInfo(const std::map<std::string, std::string> &hintsMap);
    common::QueryPtr genTableQuery(const LookupJoinBatch &batch);
    common::ColumnTerm *makeColumnTerm(const LookupJoinBatch &batch,
                                       const std::string &inputField,
                                       const std::string &joinField,
                                       bool &returnEmptyQuery);
    bool genRowGroupKey(const LookupJoinBatch &batch,
                        const std::vector<table::Column *> &singleTermCols,
                        std::vector<std::string> &rowGroupKey);
    bool genSingleTermColumns(
            const table::TablePtr &input,
            const std::vector<table::Column*> &singleTermCols,
            const std::vector<uint16_t> &singleTermID,
            const std::unordered_map<std::string, std::vector<size_t>> &rowGroups,
            size_t rowCount, std::vector<common::ColumnTerm*> &terms);
    bool genMultiTermColumns(
            const table::TablePtr &input,
            const std::vector<table::Column*> &MultiTermCols,
            const std::vector<uint16_t> &multiTermID,
            const std::unordered_map<std::string, std::vector<size_t>> &rowGroups,
            size_t rowCount, std::vector<common::ColumnTerm*> &terms);
    void fillTerms(const LookupJoinBatch &batch, std::vector<common::ColumnTerm *> &terms);
    void fillTermsWithRowOptimized(const LookupJoinBatch &batch,
                                   std::vector<common::ColumnTerm *> &terms);
    bool scanAndJoin(StreamQueryPtr streamQuery,
                     const LookupJoinBatch &batch,
                     table::TablePtr &outputTable,
                     bool &finished);

    static bool getDocIdField(const std::vector<std::string> &inputFields,
                              const std::vector<std::string> &joinFields,
                              std::string &field);
    static bool genDocIds(const LookupJoinBatch &batch,
                          const std::string &field, std::vector<docid_t> &docids);
    static bool selectValidField(const table::TablePtr &input,
                                 std::vector<std::string> &inputFields,
                                 std::vector<std::string> &joinFields,
                                 bool enableRowDeduplicate);
    static bool genStreamQueryTerm(const table::TablePtr &input, table::Row row,
                                   const std::string &inputField,
                                   std::vector<std::string> &termVec);
    static bool getPkTriggerField(const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &joinFields,
                                  const std::map<std::string, sql::IndexInfo> &indexInfoMap,
                                  std::string &triggerField);
    bool isDocIdsOptimize();

protected:
    ScanInitParam _initParam;
    ScanBasePtr _scanBase;
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    bool _inputEof;
    LookupJoinBatch _batch;
    std::shared_ptr<table::Table> _outputTable;
    bool _enableRowDeduplicate;
    bool _useMatchedRow;
    bool _leftTableIndexed;
    bool _disableMatchRowIndexOptimize;
    size_t _lookupBatchSize;
    size_t _lookupTurncateThreshold;
    size_t _hasJoinedCount;
    std::map<std::string, sql::IndexInfo> _leftIndexInfos;
    std::map<std::string, sql::IndexInfo> _rightIndexInfos;
    TableMeta _leftTableMeta;
    TableMeta _rightTableMeta;
    std::set<std::string> _disableCacheFields;
    std::vector<std::vector<size_t>> _rowMatchedInfo;
    search::MatchDataCollectMatchedInfo *_matchedInfo;
    std::vector<std::string> *_lookupColumns;
    std::vector<std::string> *_joinColumns;
    std::map<std::string, sql::IndexInfo> *_lookupIndexInfos;
    std::shared_ptr<StreamQuery> _streamQuery;
};

typedef std::shared_ptr<LookupJoinKernel> LookupJoinKernelPtr;
} // namespace sql
} // namespace isearch
