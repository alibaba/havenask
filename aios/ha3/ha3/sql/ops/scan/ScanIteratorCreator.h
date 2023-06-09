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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <limits>

#include "autil/Log.h"
#include "ha3/common/Query.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/AuxiliaryChainDefine.h"
#include "ha3/sql/common/IndexInfo.h" // IWYU pragma: keep
#include "ha3/sql/common/FieldInfo.h"
#include "ha3/sql/ops/sort/SortInitParam.h"
#include "ha3/sql/ops/scan/ScanIterator.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/turing/common/SortDescs.h"
#include "matchdoc/MatchDocAllocator.h"
#include "indexlib/config/index_schema.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace build_service {
namespace analyzer {
class AnalyzerFactory;
} // namespace analyzer
} // namespace build_service
namespace isearch {
namespace sql {
class UdfToQueryManager;
} // namespace sql

namespace common {
class QueryInfo;
} // namespace common
namespace search {
class QueryExecutor;
} // namespace search
} // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class TimeoutTerminator;
} // namespace turing
} // namespace suez

namespace isearch {
namespace sql {

struct ScanIteratorCreatorParam {
    ScanIteratorCreatorParam()
        : analyzerFactory(NULL)
        , queryInfo(NULL)
        , pool(NULL)
        , timeoutTerminator(NULL)
        , parallelIndex(0)
        , parallelNum(1)
        , modelConfigMap(NULL)
        , udfToQueryManager(NULL)
        , limit(std::numeric_limits<uint32_t>::max())
        , tableSortDescription(nullptr)
        , forbidIndexs(nullptr)
    {}
    std::string tableName;
    std::string auxTableName;
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    suez::turing::AttributeExpressionCreatorPtr attributeExpressionCreator;
    matchdoc::MatchDocAllocatorPtr matchDocAllocator;
    const build_service::analyzer::AnalyzerFactory *analyzerFactory;
    const common::QueryInfo *queryInfo;
    suez::turing::TableInfoPtr tableInfo;
    autil::mem_pool::Pool *pool;
    autil::TimeoutTerminator *timeoutTerminator;
    uint32_t parallelIndex;
    uint32_t parallelNum;
    isearch::turing::ModelConfigMap *modelConfigMap;
    UdfToQueryManager *udfToQueryManager;
    std::string truncateDesc;
    std::string matchDataLabel;
    uint32_t limit;
    SortInitParam sortDesc;
    std::unordered_map<std::string, std::vector<turing::Ha3SortDesc>> *tableSortDescription;
    const std::unordered_set<std::string> *forbidIndexs;
};

struct CreateScanIteratorInfo {
    common::QueryPtr query;
    search::FilterWrapperPtr filterWrapper;
    std::vector<suez::turing::AttributeExpression *> queryExprs;
    search::LayerMetaPtr layerMeta;
    search::MatchDataManagerPtr matchDataManager;
};

class ScanIteratorCreator {
public:
    ScanIteratorCreator(const ScanIteratorCreatorParam &param);
    ~ScanIteratorCreator();

private:
    ScanIteratorCreator(const ScanIteratorCreator &);
    ScanIteratorCreator &operator=(const ScanIteratorCreator &);

public:
    bool genCreateScanIteratorInfo(const std::string &conditionInfo,
                                   const std::map<std::string, IndexInfo> &indexInfo,
                                   const std::map<std::string, FieldInfo> &fieldInfo,
                                   CreateScanIteratorInfo &info);
    ScanIteratorPtr createScanIterator(CreateScanIteratorInfo &info,
            bool useSub,
            bool &emptyScan);
    ScanIteratorPtr createScanIterator(const common::QueryPtr &query,
            const search::FilterWrapperPtr &filterWrapper,
            const std::vector<suez::turing::AttributeExpression *> &queryExprs,
            const search::LayerMetaPtr &layerMeta,
            const search::MatchDataManagerPtr &matchDataManager,
            bool useSub,
            bool &emptyScan);

private:
    ScanIterator *createRangeScanIterator(const search::FilterWrapperPtr &filterWrapper,
                                          const search::LayerMetaPtr &layerMeta);
    ScanIterator *
    createHa3ScanIterator(const common::QueryPtr &query,
                          const search::FilterWrapperPtr &filterWrapper,
                          search::IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
                          const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                          autil::TimeoutTerminator *timeoutTerminator,
                          const search::LayerMetaPtr &layerMeta,
                          const search::MatchDataManagerPtr &matchDataManager,
                          bool useSub,
                          bool &emptyScan);
    ScanIterator *createOrderedHa3ScanIterator(
            const common::QueryPtr &query,
            const search::FilterWrapperPtr &filterWrapper,
            search::IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
            const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
            autil::TimeoutTerminator *timeoutTerminator,
            const std::vector<search::LayerMetaPtr> &layerMeta,
            const search::MatchDataManagerPtr &matchDataManager,
            bool useSub,
            bool &emptyScan);

    ScanIterator *createQueryScanIterator(const common::QueryPtr &query,
            const search::FilterWrapperPtr &filterWrapper,
            const search::LayerMetaPtr &layerMeta,
            bool useSub,
            bool &emptyScan);

private:
    bool isTermQuery(const common::QueryPtr &query);
    bool createQueryExecutors(
            const common::QueryPtr &query,
            search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
            const std::string &mainTableName,
            autil::mem_pool::Pool *pool,
            autil::TimeoutTerminator *timeoutTerminator,
            const std::vector<search::LayerMetaPtr> &layerMetas,
            search::MatchDataManager *matchDataManager,
            bool useSub,
            bool &emptyScan,
            std::vector<search::QueryExecutorPtr> &queryExecutors);
    static search::QueryExecutor *
    createQueryExecutor(const common::QueryPtr &query,
                        search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
                        const std::string &mainTableName,
                        autil::mem_pool::Pool *pool,
                        autil::TimeoutTerminator *timeoutTerminator,
                        search::LayerMeta *layerMeta,
                        search::MatchDataManager *matchDataManager,
                        bool &emptyScan);

    static search::LayerMetaPtr
    createLayerMeta(search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
                    autil::mem_pool::Pool *pool,
                    uint32_t index = 0,
                    uint32_t num = 1,
                    uint32_t limit = std::numeric_limits<uint32_t>::max());
    static search::LayerMetaPtr splitLayerMeta(autil::mem_pool::Pool *pool,
                                               const search::LayerMetaPtr &layerMeta,
                                               uint32_t index,
                                               uint32_t num);

    static bool parseIndexMap(const std::string &indexStr,
                              autil::mem_pool::Pool *pool,
                              std::map<std::string, std::string> &attrIndexMap);

    static bool createFilterWrapper(suez::turing::AttributeExpression *attrExpr,
                                    const std::vector<suez::turing::AttributeExpression *> &queryExprVec,
                                    suez::turing::JoinDocIdConverterCreator *joinDocIdConverterCreator,
                                    matchdoc::MatchDocAllocator *matchDocAllocator,
                                    autil::mem_pool::Pool *pool,
                                    search::FilterWrapperPtr &filterWrapper);
    void truncateQuery(common::QueryPtr &query);
    static bool parseTruncateDesc(const std::string &truncateDesc,
                                  std::vector<std::string> &truncateNames,
                                  std::vector<search::SelectAuxChainType> &types);
    static void subMatchQuery(common::QueryPtr &query,
                              const std::shared_ptr<indexlibv2::config::ITabletSchema> &indexSchemaPtr,
                              const std::string &matchDataLabel);
    static void proportionalLayerQuota(search::LayerMeta &layerMeta);
    bool needHa3Scan(const common::QueryPtr &query,
                     const search::MatchDataManagerPtr &matchDataManager);
    ScanIterator *createDocIdScanIterator(const common::QueryPtr &query);
    bool createJoinDocIdConverter();
private:
    std::string _tableName;
    std::string _auxTableName;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    const build_service::analyzer::AnalyzerFactory *_analyzerFactory;
    const common::QueryInfo *_queryInfo;
    suez::turing::TableInfoPtr _tableInfo;
    autil::mem_pool::Pool *_pool;
    autil::TimeoutTerminator *_timeoutTerminator;
    uint32_t _parallelIndex;
    uint32_t _parallelNum;
    isearch::turing::ModelConfigMap *_modelConfigMap;
    UdfToQueryManager *_udfToQueryManager;
    std::string _truncateDesc;
    std::string _matchDataLabel;
    uint32_t _limit;
    SortInitParam _sortDesc;
    std::unordered_map<std::string, std::vector<turing::Ha3SortDesc>> *_tableSortDescription;
    const std::unordered_set<std::string> *_forbidIndexs;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScanIteratorCreator> ScanIteratorCreatorPtr;
} // namespace sql
} // namespace isearch
