#pragma once
#include <map>
#include <memory>
#include <string>

#include "autil/mem_pool/Pool.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/partition/index_application.h"
#include "matchdoc/MatchDocAllocator.h"
#include "sql/common/IndexInfo.h"
#include "sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryManager.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace sql {
class UdfToQueryParam;

class ScanConditionTestUtil {
public:
    ~ScanConditionTestUtil() {
        _matchDocAllocator.reset();
        _indexPartitionMap.clear();
        _indexApp.reset();
    }
    void init(const std::string &path, const std::string &testDataPath);

private:
    build_service::analyzer::AnalyzerFactoryPtr
    initAnalyzerFactory(const std::string &testDataPath);
    void initParam();
    isearch::search::IndexPartitionReaderWrapperPtr getIndexPartitionReaderWrapper();
    void initIndexPartition();

public:
    std::shared_ptr<UdfToQueryParam> createUdfToQueryParam() const;

private:
    std::string _tableName;
    std::string _testPath;
    indexlib::partition::IndexApplication::IndexPartitionMap _indexPartitionMap;
    indexlib::partition::IndexApplicationPtr _indexApp;
    autil::mem_pool::Pool _pool;
    suez::turing::TableInfoPtr _tableInfo;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    build_service::analyzer::AnalyzerFactoryPtr _analyzerFactory;
    isearch::common::QueryInfo _queryInfo;
    indexlib::partition::PartitionReaderSnapshotPtr _indexAppSnapshot;
    isearch::search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    isearch::search::LayerMetaPtr _layerMeta;
    std::map<std::string, IndexInfo> _indexInfoMap;
    UdfToQueryManager _udfToQueryManager;
    Ha3ScanConditionVisitorParam _param;
};

} // namespace sql
