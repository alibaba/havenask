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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/common/ObjectPool.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanIterator.h"
#include "ha3/sql/ops/scan/ScanIteratorCreator.h"
#include "matchdoc/MatchDocAllocator.h"
#include "table/Table.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
class SyntaxExpr;
class IndexInfoHelper;
} // namespace turing
} // namespace suez

namespace indexlib {
namespace util {
struct BlockAccessCounter;
} // namespace util

namespace index {
class InvertedIndexSearchTracer;
} // namespace index

} // namespace indexlib


namespace isearch {
namespace sql {

class NormalScan : public ScanBase {
public:
    NormalScan(const ScanInitParam &param);
    virtual ~NormalScan();

private:
    NormalScan(const NormalScan &);
    NormalScan &operator=(const NormalScan &);

public:
    bool isDocIdsOptimize() { return _isDocIdsOptimize; }

protected:
    void patchHintInfo(const std::map<std::string, std::string> &hintMap) override;
private:
    bool doInit() override;
    bool postInitPushDownOp(PushDownOp &pushDownOp) override;
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    std::shared_ptr<matchdoc::MatchDocAllocator> copyMatchDocAllocator(
            std::vector<matchdoc::MatchDoc> &matchDocVec,
            const std::shared_ptr<matchdoc::MatchDocAllocator> &matchDocAllocator,
            bool reuseMatchDocAllocator,
            std::vector<matchdoc::MatchDoc> &copyMatchDocs) override;
    std::shared_ptr<table::Table>
    doCreateTable(std::shared_ptr<matchdoc::MatchDocAllocator> outputAllocator,
                  std::vector<matchdoc::MatchDoc> copyMatchDocs) override;
    bool initOutputColumn();
    bool copyField(const std::string &expr, const std::string &outputName,
                   std::map<std::string, std::pair<std::string, bool> > &expr2Outputs);
    suez::turing::AttributeExpression*
    initAttributeExpr(const std::string& tableName, const std::string& outputName,const std::string& outputFieldType,const std::string& exprStr,
                      std::map<std::string, std::pair<std::string, bool> >& expr2Outputs);
    void evaluateAttribute(std::vector<matchdoc::MatchDoc> &matchDocVec,
                           matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                           std::vector<suez::turing::AttributeExpression *> &attributeExpressionVec,
                           bool eof);
    void flattenSub(matchdoc::MatchDocAllocatorPtr &outputAllocator,
                    const std::vector<matchdoc::MatchDoc> &copyMatchDocs,
                    table::TablePtr &table);
    bool appendCopyColumn(const std::string &srcField,
                          const std::string &destField,
                          matchdoc::MatchDocAllocatorPtr &matchDocAllocator);
    void copyColumns(const std::map<std::string, std::string> &copyFieldMap,
                     const std::vector<matchdoc::MatchDoc> &matchDocVec,
                     matchdoc::MatchDocAllocatorPtr &matchDocAllocator);
    std::pair<suez::turing::SyntaxExpr*, bool> parseSyntaxExpr(const std::string& tableName, const std::string& exprStr);
    void requireMatchData();
    void getInvertedTracers(std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap);
    void collectInvertedInfos(
        const std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap);
    void collectBlockInfo(const indexlib::util::BlockAccessCounter *counter,
                          isearch::sql::BlockAccessInfo &blockInfo);
    void reportInvertedMetrics(
        std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap);

private:
    std::map<std::string, std::string> _copyFieldMap;
    std::vector<suez::turing::AttributeExpression *> _attributeExpressionVec;
    ScanIteratorPtr _scanIter;
    ScanIteratorCreatorPtr _scanIterCreator;
    CreateScanIteratorInfo _baseCreateScanIterInfo;
    ObjectPoolReadOnlyPtr _objectPoolReadOnly;
    NestTableJoinType _nestTableJoinType;
    std::string _truncateDesc;
    std::string _matchDataLabel;
    int32_t _updateQueryCount = 0;
    bool _notUseMatchData;
    const suez::turing::IndexInfoHelper *_indexInfoHelper;
    bool _isDocIdsOptimize;
    std::map<std::string, indexlib::index::InvertedIndexSearchTracer> _tracerMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NormalScan> NormalScanPtr;
} // namespace sql
} // namespace isearch
