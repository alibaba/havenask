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

#include <engine/ResourceInitContext.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ha3/search/MatchDataManager.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/common/common.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanIterator.h"
#include "sql/ops/scan/ScanIteratorCreatorR.h"
#include "sql/ops/scan/ScanPushDownR.h"
#include "sql/ops/scan/UseSubR.h"

namespace indexlib {
namespace index {
class InvertedIndexSearchTracer;
} // namespace index
namespace util {
struct BlockAccessCounter;
} // namespace util
} // namespace indexlib
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc
namespace navi {
class ResourceDefBuilder;
} // namespace navi
namespace suez {
namespace turing {
class AttributeExpression;
class SyntaxExpr;
} // namespace turing
} // namespace suez
namespace table {
class Table;
} // namespace table

namespace sql {
class BlockAccessInfo;

class NormalScanR : public ScanBase {
public:
    NormalScanR();
    ~NormalScanR();
    NormalScanR(const NormalScanR &) = delete;
    NormalScanR &operator=(const NormalScanR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

public:
    const std::shared_ptr<matchdoc::MatchDocAllocator> &getMatchDocAllocator() const {
        return _attributeExpressionCreatorR->_matchDocAllocator;
    }
    const isearch::search::MatchDataManagerPtr &getMatchDataManager() const {
        return _scanIteratorCreatorR->getMatchDataManager();
    }
    bool isDocIdsOptimize() {
        return _isDocIdsOptimize;
    }
    const ScanPushDownRPtr &getScanPushDownR() const {
        return _scanPushDownR;
    }

private:
    bool initPushDown(navi::ResourceInitContext &ctx);
    void postInitPushDown();
    void initNestTableJoinType();
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    std::shared_ptr<matchdoc::MatchDocAllocator>
    copyMatchDocAllocator(std::vector<matchdoc::MatchDoc> &matchDocVec,
                          const std::shared_ptr<matchdoc::MatchDocAllocator> &matchDocAllocator,
                          bool reuseMatchDocAllocator,
                          std::vector<matchdoc::MatchDoc> &copyMatchDocs) override;
    std::shared_ptr<table::Table>
    doCreateTable(std::shared_ptr<matchdoc::MatchDocAllocator> outputAllocator,
                  std::vector<matchdoc::MatchDoc> copyMatchDocs) override;
    bool initOutputColumn();
    bool copyField(const std::string &expr,
                   const std::string &outputName,
                   std::map<std::string, std::pair<std::string, bool>> &expr2Outputs);
    suez::turing::AttributeExpression *
    initAttributeExpr(const std::string &tableName,
                      const std::string &outputName,
                      const std::string &outputFieldType,
                      const std::string &exprStr,
                      std::map<std::string, std::pair<std::string, bool>> &expr2Outputs);
    void evaluateAttribute(std::vector<matchdoc::MatchDoc> &matchDocVec,
                           const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                           std::vector<suez::turing::AttributeExpression *> &attributeExpressionVec,
                           bool eof);
    void flattenSub(matchdoc::MatchDocAllocatorPtr &outputAllocator,
                    const std::vector<matchdoc::MatchDoc> &copyMatchDocs,
                    table::TablePtr &table);
    std::pair<suez::turing::SyntaxExpr *, bool> parseSyntaxExpr(const std::string &tableName,
                                                                const std::string &exprStr);
    void getInvertedTracers(
        std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap);
    void collectInvertedInfos(
        const std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap);
    void collectBlockInfo(const indexlib::util::BlockAccessCounter *counter,
                          BlockAccessInfo &blockInfo);
    void reportInvertedMetrics(
        std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap);

private:
    static bool appendCopyColumn(const std::string &srcField,
                                 const std::string &destField,
                                 const matchdoc::MatchDocAllocatorPtr &matchDocAllocator);
    static void copyColumns(const std::map<std::string, std::string> &copyFieldMap,
                            const std::vector<matchdoc::MatchDoc> &matchDocVec,
                            const matchdoc::MatchDocAllocatorPtr &matchDocAllocator);

private:
    RESOURCE_DEPEND_DECLARE_BASE(ScanBase);

private:
    RESOURCE_DEPEND_ON(AttributeExpressionCreatorR, _attributeExpressionCreatorR);
    RESOURCE_DEPEND_ON(ScanIteratorCreatorR, _scanIteratorCreatorR);
    RESOURCE_DEPEND_ON(UseSubR, _useSubR);
    navi::ResourceInitContext _ctx;
    bool _enableScanTimeout = true;
    std::map<std::string, std::string> _copyFieldMap;
    std::vector<suez::turing::AttributeExpression *> _attributeExpressionVec;
    ScanIteratorPtr _scanIter;
    bool _isDocIdsOptimize = false;
    NestTableJoinType _nestTableJoinType = LEFT_JOIN;
    std::map<std::string, indexlib::index::InvertedIndexSearchTracer> _tracerMap;
    ScanPushDownRPtr _scanPushDownR;
};

NAVI_TYPEDEF_PTR(NormalScanR);

} // namespace sql
