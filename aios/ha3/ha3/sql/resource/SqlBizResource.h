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

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/SharedObjectMap.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/turing/common/ModelConfig.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "indexlib/partition/index_application.h"
#include "ha3/turing/common/SortDescs.h"

namespace multi_call {
class SearchService;
typedef std::shared_ptr<SearchService> SearchServicePtr;
} // namespace multi_call
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace resource_reader {
class ResourceReader;

typedef std::shared_ptr<ResourceReader> ResourceReaderPtr;
} // namespace resource_reader

namespace suez {
class IndexProvider;
namespace turing {
class Biz;
class CavaPluginManager;
class FunctionInterfaceCreator;
class TableInfo;

typedef std::shared_ptr<CavaPluginManager> CavaPluginManagerPtr;
typedef std::shared_ptr<FunctionInterfaceCreator> FunctionInterfaceCreatorPtr;
typedef std::shared_ptr<TableInfo> TableInfoPtr;
} // namespace turing
} // namespace suez

namespace isearch {
namespace sql {
class AggFuncManager;
class TvfFuncManager;
class UdfToQueryManager;
class TvfFuncCreatorR;
class MessageWriterManager;
typedef std::shared_ptr<AggFuncManager> AggFuncManagerPtr;
typedef std::shared_ptr<TvfFuncManager> TvfFuncManagerPtr;
typedef std::shared_ptr<UdfToQueryManager> UdfToQueryManagerPtr;
typedef std::shared_ptr<MessageWriterManager> MessageWriterManagerPtr;
} // namespace sql
} // namespace isearch

namespace build_service {
namespace analyzer {
class AnalyzerFactory;

typedef std::shared_ptr<AnalyzerFactory> AnalyzerFactoryPtr;
} // namespace analyzer
} // namespace build_service

namespace future_lite {
class Executor;
}

namespace turbojet {
class TraitObject;
}

namespace isearch {
namespace sql {

class SqlBizResource : public navi::Resource {
public:
    SqlBizResource();
    ~SqlBizResource();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    void setAggFuncManager(const sql::AggFuncManagerPtr &aggFunManager);
    sql::AggFuncManager *getAggFuncManager() const;

    void setTvfFuncManager(const sql::TvfFuncManagerPtr &tvfFunManager);
    sql::TvfFuncManager *getTvfFuncManager() const;

    void setUdfToQueryManager(const sql::UdfToQueryManagerPtr &udfToQueryManager);
    sql::UdfToQueryManager *getUdfToQueryManager() const;

    void setAnalyzerFactory(const build_service::analyzer::AnalyzerFactoryPtr &analyzerFactory);
    build_service::analyzer::AnalyzerFactory *getAnalyzerFactory() const;

    void setQueryInfo(const common::QueryInfo &queryInfo);
    const common::QueryInfo *getQueryInfo() const;

    void setTableInfo(const suez::turing::TableInfoPtr &tableInfo);
    suez::turing::TableInfo *getTableInfo() const;
    suez::turing::TableInfo *getTableInfo(const std::string &tableName) const;
    void setTableInfoWithRel(const suez::turing::TableInfoPtr &tableInfoWithRel);
    suez::turing::TableInfoPtr getTableInfoWithRel() const;

    void setDependencyTableInfoMap(
        const std::map<std::string, suez::turing::TableInfoPtr> &tableInfoMap);
    const std::map<std::string, suez::turing::TableInfoPtr> *getDependencyTableInfoMap() const;

    void setFunctionInterfaceCreator(const suez::turing::FunctionInterfaceCreatorPtr &creator);
    suez::turing::FunctionInterfaceCreator *getFunctionInterfaceCreator() const;

    void setCavaPluginManager(const suez::turing::CavaPluginManagerPtr &cavaPluginManager);
    suez::turing::CavaPluginManager *getCavaPluginManager() const;

    void setTurboJetCalcCompiler(const std::shared_ptr<turbojet::TraitObject> &calcCompiler);
    turbojet::TraitObject *getTurboJetCalcCompiler() const;

    void setResourceReader(resource_reader::ResourceReaderPtr &resourceReader);
    resource_reader::ResourceReader *getResourceReader() const;

    void setAsyncInterExecutor(future_lite::Executor *asyncExecutor);
    future_lite::Executor *getAsyncInterExecutor() const;

    void setAsyncIntraExecutor(future_lite::Executor *asyncExecutor);
    future_lite::Executor *getAsyncIntraExecutor() const;

    void setTableSortDescription(const std::unordered_map<
            std::string, std::vector<turing::Ha3SortDesc>> &tableSortDescMap);
    std::unordered_map<std::string, std::vector<turing::Ha3SortDesc>> *getTableSortDescription();

    template <typename T>
    T *getObject(const std::string &name) const {
        T *object = nullptr;
        if (!_sharedObjectMap.get<T>(name, object)) {
            SQL_LOG(WARN, "get shared object failed, name [%s]", name.c_str());
            return nullptr;
        }
        return object;
    }
    autil::SharedObjectMap *getSharedObjectMap() {
        return &_sharedObjectMap;
    }

    void setModelConfigMap(isearch::turing::ModelConfigMap *modelConfigMap) {
        _modelConfigMap = modelConfigMap;
    }
    isearch::turing::ModelConfigMap *getModelConfigMap() const {
        return _modelConfigMap;
    }
    void setTvfNameToCreator(std::map<std::string, isearch::sql::TvfFuncCreatorR *> *creator) {
        _tvfNameToCreator = creator;
    }
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> *getTvfNameToCreator() const {
        return _tvfNameToCreator;
    }

    multi_call::SearchServicePtr getSearchService() {
        return _searcheService;
    }
    void setSearchService(multi_call::SearchServicePtr searchService) {
        _searcheService = searchService;
    }

    void setIndexAppMap(const std::map<int32_t, indexlib::partition::IndexApplicationPtr> *indexAppMap) {
        _id2IndexAppMap = indexAppMap;
    }

    const std::map<int32_t, indexlib::partition::IndexApplicationPtr> *getIndexAppMap() const {
        return _id2IndexAppMap;
    }

    void setIndexProvider(const suez::IndexProvider *indexProvider) {
        _indexProvider = indexProvider;
    }

    const suez::IndexProvider *getIndexProvider() const {
        return _indexProvider;
    }

    indexlib::partition::PartitionReaderSnapshotPtr
    createDefaultIndexSnapshot(const std::string &leadingTableName = "") {
        if (_id2IndexAppMap == nullptr) {
            return {};
        }
        if (_id2IndexAppMap->size() > 0) {
            auto indexApp = _id2IndexAppMap->begin()->second;
            if (indexApp) {
                return indexApp->CreateSnapshot(leadingTableName, _cacheSnapshotTime);
            }
        }
        return indexlib::partition::PartitionReaderSnapshotPtr();
        }

    indexlib::partition::PartitionReaderSnapshotPtr
    createPartitionReaderSnapshot(int32_t partid, const std::string &leadingTableName = "") {
        if (_id2IndexAppMap == nullptr) {
            return {};
        }
        auto iter = _id2IndexAppMap->find(partid);
        if (iter != _id2IndexAppMap->end()) {
            auto indexApp = iter->second;
            if (indexApp) {
                return indexApp->CreateSnapshot(leadingTableName, _cacheSnapshotTime);
            }
        }
        AUTIL_LOG(DEBUG, "not find part id [%d] index snapshot, use default", partid);
        return createDefaultIndexSnapshot(leadingTableName);
    }

    void setBizName(const std::string &bizName);
    const std::string &getBizName() const;
    void setCavaAllocSizeLimit(size_t cavaAllocSizeLimit);
    size_t getCavaAllocSizeLimit() const;
    void setServiceSnapshot(std::shared_ptr<void> serviceSnapshot);
private:
    std::shared_ptr<void> _serviceSnapshot;
    build_service::analyzer::AnalyzerFactoryPtr _analyzerFactory;
    common::QueryInfo _queryInfo;
    sql::AggFuncManagerPtr _aggFuncManager;
    sql::TvfFuncManagerPtr _tvfFuncManager;
    sql::UdfToQueryManagerPtr _udfToQueryManager;
    autil::SharedObjectMap _sharedObjectMap;
    suez::turing::TableInfoPtr _tableInfo; // compatible with ha3 qrs
    std::map<std::string, suez::turing::TableInfoPtr> _dependencyTableInfoMap;
    suez::turing::TableInfoPtr _tableInfoWithRel;
    suez::turing::FunctionInterfaceCreatorPtr _functionInterfaceCreator;
    suez::turing::CavaPluginManagerPtr _cavaPluginManager;
    std::shared_ptr<turbojet::TraitObject> _calcCompiler;
    resource_reader::ResourceReaderPtr _resourceReader;
    future_lite::Executor *_asyncInterExecutor = nullptr;
    future_lite::Executor *_asyncIntraExecutor = nullptr;
    std::unordered_map<std::string, std::vector<turing::Ha3SortDesc>> _tableSortDescMap;
    isearch::turing::ModelConfigMap *_modelConfigMap = nullptr;
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> *_tvfNameToCreator = nullptr;
    multi_call::SearchServicePtr _searcheService;
    const std::map<int32_t, indexlib::partition::IndexApplicationPtr> *_id2IndexAppMap = nullptr;
    const suez::IndexProvider *_indexProvider = nullptr;
    size_t _cavaAllocSizeLimit = 8u; // MB
    std::string _bizName;
    int64_t _cacheSnapshotTime = -1;
private:
    AUTIL_LOG_DECLARE();

public:
    static const std::string RESOURCE_ID;
};

NAVI_TYPEDEF_PTR(SqlBizResource);

} // namespace sql
} // namespace isearch
