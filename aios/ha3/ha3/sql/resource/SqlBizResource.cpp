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
#include "ha3/sql/resource/SqlBizResource.h"

#include <iosfwd>
#include <utility>

#include "autil/EnvUtil.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/common.h"

#include "future_lite/Executor.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, SqlBizResource);

const string SqlBizResource::RESOURCE_ID = "SqlBizResource";

SqlBizResource::SqlBizResource() {
    _cacheSnapshotTime = autil::EnvUtil::getEnv<int64_t>("cache_partition_snapshot_time_us", 500 * 1000); // cache snapshot 500ms
}

SqlBizResource::~SqlBizResource() {
    NAVI_KERNEL_LOG(INFO, "sql biz resource destructed [%p]", this);
}

void SqlBizResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

navi::ErrorCode SqlBizResource::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

void SqlBizResource::setAggFuncManager(const sql::AggFuncManagerPtr &aggFunManager) {
    _aggFuncManager = aggFunManager;
}
sql::AggFuncManager *SqlBizResource::getAggFuncManager() const {
    return _aggFuncManager.get();
}

void SqlBizResource::setTvfFuncManager(const sql::TvfFuncManagerPtr &tvfFunManager) {
    _tvfFuncManager = tvfFunManager;
}
sql::TvfFuncManager *SqlBizResource::getTvfFuncManager() const {
    return _tvfFuncManager.get();
}

void SqlBizResource::setUdfToQueryManager(const sql::UdfToQueryManagerPtr &udfToQueryManager) {
    _udfToQueryManager = udfToQueryManager;
}
sql::UdfToQueryManager *SqlBizResource::getUdfToQueryManager() const {
    return _udfToQueryManager.get();
}

void SqlBizResource::setAnalyzerFactory(
    const build_service::analyzer::AnalyzerFactoryPtr &analyzerFactory) {
    _analyzerFactory = analyzerFactory;
}
build_service::analyzer::AnalyzerFactory *SqlBizResource::getAnalyzerFactory() const {
    return _analyzerFactory.get();
}

void SqlBizResource::setQueryInfo(const common::QueryInfo &queryInfo) {
    _queryInfo = queryInfo;
}
const common::QueryInfo *SqlBizResource::getQueryInfo() const {
    return &_queryInfo;
}

void SqlBizResource::setTableInfo(const TableInfoPtr &tableInfo) {
    _tableInfo = tableInfo;
}

TableInfo *SqlBizResource::getTableInfo() const {
    return _tableInfo.get();
}

TableInfo *SqlBizResource::getTableInfo(const string &tableName) const {
    auto tableInfo = _dependencyTableInfoMap.find(tableName);
    if (tableInfo != _dependencyTableInfoMap.end()) {
        return tableInfo->second.get();
    }
    SQL_LOG(WARN, "get table info failed, tableName [%s]", tableName.c_str());
    return nullptr;
}

void SqlBizResource::setTableInfoWithRel(const TableInfoPtr &tableInfoWithRel) {
    _tableInfoWithRel = tableInfoWithRel;
}
TableInfoPtr SqlBizResource::getTableInfoWithRel() const {
    return _tableInfoWithRel;
}

void SqlBizResource::setDependencyTableInfoMap(const map<string, TableInfoPtr> &tableInfoMap) {
    _dependencyTableInfoMap = tableInfoMap;
}
const map<string, TableInfoPtr> *SqlBizResource::getDependencyTableInfoMap() const {
    return &_dependencyTableInfoMap;
}

void SqlBizResource::setFunctionInterfaceCreator(const FunctionInterfaceCreatorPtr &creator) {
    _functionInterfaceCreator = creator;
}
FunctionInterfaceCreator *SqlBizResource::getFunctionInterfaceCreator() const {
    return _functionInterfaceCreator.get();
}

void SqlBizResource::setCavaPluginManager(const CavaPluginManagerPtr &cavaPluginManager) {
    _cavaPluginManager = cavaPluginManager;
}
CavaPluginManager *SqlBizResource::getCavaPluginManager() const {
    return _cavaPluginManager.get();
}

void SqlBizResource::setTurboJetCalcCompiler(
    const std::shared_ptr<turbojet::TraitObject> &calcCompiler) {
    _calcCompiler = calcCompiler;
}
turbojet::TraitObject *SqlBizResource::getTurboJetCalcCompiler() const {
    return _calcCompiler.get();
}

void SqlBizResource::setResourceReader(resource_reader::ResourceReaderPtr &resourceReader) {
    _resourceReader = resourceReader;
}
resource_reader::ResourceReader *SqlBizResource::getResourceReader() const {
    return _resourceReader.get();
}

void SqlBizResource::setAsyncInterExecutor(future_lite::Executor *asyncExecutor) {
    _asyncInterExecutor = asyncExecutor;
}

future_lite::Executor *SqlBizResource::getAsyncInterExecutor() const {
    return _asyncInterExecutor;
}

void SqlBizResource::setAsyncIntraExecutor(future_lite::Executor *asyncExecutor) {
    _asyncIntraExecutor = asyncExecutor;
}

future_lite::Executor *SqlBizResource::getAsyncIntraExecutor() const {
    return _asyncIntraExecutor;
}

void SqlBizResource::setTableSortDescription(
        const unordered_map<string, vector<turing::Ha3SortDesc>> &tableSortDescMap)
{
    _tableSortDescMap = tableSortDescMap;
}

unordered_map<string, vector<turing::Ha3SortDesc>> *
SqlBizResource::getTableSortDescription()
{
    return &_tableSortDescMap;
}

void SqlBizResource::setBizName(const std::string &bizName) {
    _bizName = bizName;
}

const std::string &SqlBizResource::getBizName() const {
    return _bizName;
}


void SqlBizResource::setCavaAllocSizeLimit(size_t cavaAllocSizeLimit) {
    _cavaAllocSizeLimit = cavaAllocSizeLimit;
}

size_t SqlBizResource::getCavaAllocSizeLimit() const {
    return _cavaAllocSizeLimit;
}


void SqlBizResource::setServiceSnapshot(std::shared_ptr<void> serviceSnapshot) {
    _serviceSnapshot = serviceSnapshot;
}

} // namespace sql
} // namespace isearch
