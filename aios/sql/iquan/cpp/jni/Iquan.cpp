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
#include "iquan/jni/Iquan.h"

#include "iquan/jni/IquanImpl.h"

namespace iquan {

Iquan::Iquan() : _impl(new IquanImpl()) {}

Iquan::~Iquan() { _impl.reset(); }

Status Iquan::init(const JniConfig &jniConfig, const ClientConfig &sqlConfig) {
    return _impl->init(jniConfig, sqlConfig);
}

Status Iquan::updateTables(const TableModels &tables) { return _impl->updateTables(tables); }

Status Iquan::updateTables(const std::string &tableContent) { return _impl->updateTables(tableContent); }

Status Iquan::updateLayerTables(const LayerTableModels &tables) { return _impl->updateLayerTables(tables); }

Status Iquan::updateLayerTables(const std::string &tableContent) { return _impl->updateLayerTables(tableContent); }

Status Iquan::updateFunctions(FunctionModels &functions) { return _impl->updateFunctions(functions); }

Status Iquan::updateFunctions(const TvfModels &functions) { return _impl->updateFunctions(functions); }

Status Iquan::updateFunctions(const std::string &functionContent) { return _impl->updateFunctions(functionContent); }

Status Iquan::updateCatalog(const CatalogInfo &catalog) { return _impl->updateCatalog(catalog); }

Status Iquan::query(IquanDqlRequest &request, IquanDqlResponse &response, PlanCacheStatus &planCacheStatus) { return _impl->query(request, response, planCacheStatus); }

Status Iquan::listCatalogs(std::string &result) { return _impl->listCatalogs(result); }

Status Iquan::listDatabases(const std::string &catalogName, std::string &result) {
    return _impl->listDatabases(catalogName, result);
}

Status Iquan::listTables(const std::string &catalogName, const std::string &dbName, std::string &result) {
    return _impl->listTables(catalogName, dbName, result);
}

Status Iquan::listFunctions(const std::string &catalogName, const std::string &dbName, std::string &result) {
    return _impl->listFunctions(catalogName, dbName, result);
}

Status Iquan::getTableDetails(const std::string &catalogName,
                              const std::string &dbName,
                              const std::string &tableName,
                              std::string &result) {
    return _impl->getTableDetails(catalogName, dbName, tableName, result);
}

Status Iquan::getFunctionDetails(const std::string &catalogName,
                                 const std::string &dbName,
                                 const std::string &functionName,
                                 std::string &result) {
    return _impl->getFunctionDetails(catalogName, dbName, functionName, result);
}

Status Iquan::dumpCatalog(std::string &result) { return _impl->dumpCatalog(result); }

Status Iquan::warmup(const WarmupConfig &warmupConfig) { return _impl->warmup(warmupConfig); }

uint64_t Iquan::getPlanCacheKeyCount() { return _impl->getPlanCacheKeyCount(); }

uint64_t Iquan::getPlanMetaCacheKeyCount() { return _impl->getPlanMetaCacheKeyCount(); }

void Iquan::resetPlanCache() { return _impl->resetPlanCache(); };

void Iquan::resetPlanMetaCache() { return _impl->resetPlanMetaCache(); };
} // namespace iquan
