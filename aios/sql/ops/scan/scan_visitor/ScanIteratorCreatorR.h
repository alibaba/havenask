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
#include <stdint.h>
#include <string>
#include <vector>

#include "ha3/common/Query.h"
#include "ha3/search/AuxiliaryChainDefine.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanIterator.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/ops/scan/UseSubR.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryManagerR.h"
#include "sql/resource/AnalyzerFactoryR.h"
#include "sql/resource/Ha3QueryInfoR.h"
#include "sql/resource/Ha3TableInfoR.h"
#include "sql/resource/ModelConfigMapR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/navi/QueryMemPoolR.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlibv2 {
namespace config {
class ITabletSchema;
} // namespace config
} // namespace indexlibv2
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi
namespace suez {
namespace turing {
class AttributeExpression;
class IndexInfoHelper;
class JoinDocIdConverterCreator;
class MetaInfo;
} // namespace turing
} // namespace suez

namespace sql {

struct CreateScanIteratorInfo {
    isearch::common::QueryPtr query;
    isearch::search::FilterWrapperPtr filterWrapper;
    std::vector<suez::turing::AttributeExpression *> queryExprs;
    isearch::search::LayerMetaPtr layerMeta;
};

class ScanIteratorCreatorR : public navi::Resource {
public:
    ScanIteratorCreatorR();
    ~ScanIteratorCreatorR();
    ScanIteratorCreatorR(const ScanIteratorCreatorR &) = delete;
    ScanIteratorCreatorR &operator=(const ScanIteratorCreatorR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    ScanIteratorPtr createScanIterator(bool &emptyScan);
    void requireMatchData();
    bool updateQuery(const StreamQueryPtr &inputQuery);
    bool useMatchData() const {
        return _matchDataManager->getNeedMatchData();
    }
    const isearch::search::MatchDataManagerPtr &getMatchDataManager() const {
        return _matchDataManager;
    }
    const suez::turing::IndexInfoHelper *getIndexInfoHelper() const {
        return _indexInfoHelper;
    }

private:
    bool initMatchData();
    void postInitMatchData();
    ScanIterator *createRangeScanIterator(const isearch::search::FilterWrapperPtr &filterWrapper,
                                          const isearch::search::LayerMetaPtr &layerMeta);
    ScanIterator *createHa3ScanIterator(const isearch::common::QueryPtr &query,
                                        const isearch::search::FilterWrapperPtr &filterWrapper,
                                        const isearch::search::LayerMetaPtr &layerMeta,
                                        bool &emptyScan);
    ScanIterator *
    createOrderedHa3ScanIterator(const isearch::common::QueryPtr &query,
                                 const isearch::search::FilterWrapperPtr &filterWrapper,
                                 const std::vector<isearch::search::LayerMetaPtr> &layerMetas,
                                 bool &emptyScan);
    ScanIterator *createQueryScanIterator(const isearch::common::QueryPtr &query,
                                          const isearch::search::FilterWrapperPtr &filterWrapper,
                                          const isearch::search::LayerMetaPtr &layerMeta,
                                          bool &emptyScan);
    bool isTermQuery(const isearch::common::QueryPtr &query);
    bool createQueryExecutors(const isearch::common::QueryPtr &query,
                              const std::string &mainTableName,
                              const std::vector<isearch::search::LayerMetaPtr> &layerMetas,
                              bool &emptyScan,
                              std::vector<isearch::search::QueryExecutorPtr> &queryExecutors);
    isearch::search::QueryExecutor *createQueryExecutor(const isearch::common::QueryPtr &query,
                                                        const std::string &mainTableName,
                                                        isearch::search::LayerMeta *layerMeta,
                                                        bool &emptyScan);
    isearch::search::LayerMetaPtr createLayerMeta();
    isearch::search::LayerMetaPtr
    splitLayerMeta(const isearch::search::LayerMetaPtr &layerMeta, uint32_t index, uint32_t num);
    bool createFilterWrapper(suez::turing::AttributeExpression *attrExpr,
                             const std::vector<suez::turing::AttributeExpression *> &queryExprVec,
                             suez::turing::JoinDocIdConverterCreator *joinDocIdConverterCreator,
                             isearch::search::FilterWrapperPtr &filterWrapper);
    bool initTruncateDesc(isearch::common::QueryPtr &query);
    std::string getMatchDataLabel() const;
    void truncateQuery(isearch::common::QueryPtr &query, const std::string &truncateDesc);
    static bool parseTruncateDesc(const std::string &truncateDesc,
                                  std::vector<std::string> &truncateNames,
                                  std::vector<isearch::search::SelectAuxChainType> &types);
    static void
    subMatchQuery(isearch::common::QueryPtr &query,
                  const std::shared_ptr<indexlibv2::config::ITabletSchema> &indexSchemaPtr,
                  const std::string &matchDataLabel);
    static void proportionalLayerQuota(isearch::search::LayerMeta &layerMeta);
    bool needHa3Scan(const isearch::common::QueryPtr &query);
    ScanIterator *createDocIdScanIterator(const isearch::common::QueryPtr &query);
    bool createJoinDocIdConverter();
    void termCombine(isearch::common::QueryPtr &query,
                     const std::shared_ptr<indexlibv2::config::ITabletSchema> &tabletSchema);

public:
    // for test
    static isearch::search::LayerMetaPtr
    splitLayerMeta(autil::mem_pool::Pool *pool,
                   const isearch::search::LayerMetaPtr &layerMeta,
                   uint32_t index,
                   uint32_t num);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(ScanR, _scanR);
    RESOURCE_DEPEND_ON(ScanInitParamR, _scanInitParamR);
    RESOURCE_DEPEND_ON(UseSubR, _useSubR);
    RESOURCE_DEPEND_ON(suez::turing::QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(AnalyzerFactoryR, _analyzerFactoryR);
    RESOURCE_DEPEND_ON(Ha3QueryInfoR, _ha3QueryInfoR);
    RESOURCE_DEPEND_ON(TimeoutTerminatorR, _timeoutTerminatorR);
    RESOURCE_DEPEND_ON(ModelConfigMapR, _modelConfigMapR);
    RESOURCE_DEPEND_ON(UdfToQueryManagerR, _udfToQueryManagerR);
    RESOURCE_DEPEND_ON(AttributeExpressionCreatorR, _attributeExpressionCreatorR);
    RESOURCE_DEPEND_ON(Ha3TableInfoR, _ha3TableInfoR);
    isearch::search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    suez::turing::TableInfoPtr _tableInfo;
    std::shared_ptr<matchdoc::MatchDocAllocator> _matchDocAllocator;
    suez::turing::FunctionProviderPtr _functionProvider;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    CreateScanIteratorInfo _baseScanIteratorInfo;
    CreateScanIteratorInfo _createScanIteratorInfo;
    isearch::search::MatchDataManagerPtr _matchDataManager;
    std::shared_ptr<suez::turing::MetaInfo> _metaInfo;
    const suez::turing::IndexInfoHelper *_indexInfoHelper = nullptr;
    int32_t _updateQueryCount = 0;
};

NAVI_TYPEDEF_PTR(ScanIteratorCreatorR);

} // namespace sql
