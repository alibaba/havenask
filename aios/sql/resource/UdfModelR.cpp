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
#include "sql/resource/UdfModelR.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>
#include <utility>

#include "autil/StringUtil.h"
#include "iquan/common/catalog/TableModel.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/resource/Ha3FunctionModelConverter.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string UdfModelR::RESOURCE_ID = "sql.udf_model_r";

UdfModelR::UdfModelR() {}

UdfModelR::~UdfModelR() {}

void UdfModelR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool UdfModelR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "default_udf", _defaultUdfModels, _defaultUdfModels);
    NAVI_JSONIZE(ctx, "system_udf", _systemUdfModels, _systemUdfModels);
    NAVI_JSONIZE(ctx, "db_udf_map", _dbUdfMap, _dbUdfMap);
    NAVI_JSONIZE(ctx, "special_catalogs", _specialCatalogs, _specialCatalogs);
    return true;
}

navi::ErrorCode UdfModelR::init(navi::ResourceInitContext &ctx) {
    mergeUdfs(_systemUdfModels, _defaultUdfModels);
    for (const auto &pair : _dbUdfMap) {
        const auto &dbName = pair.first;
        std::vector<iquan::FunctionModel> functionModels;
        for (const auto &model : pair.second) {
            if (std::find(_systemUdfModels.begin(), _systemUdfModels.end(), model)
                == _systemUdfModels.end()) {
                functionModels.push_back(model);
            }
        }
        _dbUdfMap[dbName] = std::move(functionModels);
    }
    _dbUdfMap["system"] = _systemUdfModels;
    _specialCatalogs.push_back(SQL_DEFAULT_CATALOG_NAME);
    return navi::EC_NONE;
}

bool UdfModelR::fillFunctionModels(iquan::CatalogDefs &catalogDefs) {
    SQL_LOG(INFO,
            "add functions to [%s] catalogs",
            autil::StringUtil::toString(_specialCatalogs).c_str());
    if (_aggFuncFactoryR) {
        auto aggFunctionModels = _aggFuncFactoryR->getFunctionModels();
        auto status = Ha3FunctionModelConverter::convert(aggFunctionModels);
        if (!status.ok()) {
            SQL_LOG(ERROR, "udaf model converter failed");
            return false;
        }
        if (!addFunctions(catalogDefs, _specialCatalogs, aggFunctionModels, SQL_SYSTEM_DATABASE)) {
            SQL_LOG(ERROR, "add udaf functions failed");
            return false;
        }
        SQL_LOG(INFO, "add iquan udaf success");
    }

    if (_tvfFuncFactoryR) {
        std::vector<iquan::FunctionModel> tvfModels;
        if (!_tvfFuncFactoryR->fillTvfModels(tvfModels)) {
            SQL_LOG(ERROR, "reg tvf models failed");
            return false;
        }
        if (!addFunctions(catalogDefs, _specialCatalogs, tvfModels, SQL_SYSTEM_DATABASE)) {
            SQL_LOG(ERROR, "add tvf functions failed");
            return false;
        }
        SQL_LOG(INFO, "add iquan tvf success");
    }

    for (auto &pair : _dbUdfMap) {
        const std::string &dbName = pair.first;
        auto &dbUniqueFunctionModels = pair.second;
        auto status = Ha3FunctionModelConverter::convert(dbUniqueFunctionModels);
        if (!status.ok()) {
            SQL_LOG(ERROR, "db [%s] udf model converter failed", dbName.c_str());
            return false;
        }
        SQL_LOG(INFO, "add udf in db [%s]", dbName.c_str());
        if (!addFunctions(catalogDefs, _specialCatalogs, dbUniqueFunctionModels, dbName)) {
            SQL_LOG(ERROR, "add functions in db [%s] failed", dbName.c_str());
            return false;
        }
    }
    SQL_LOG(INFO, "add iquan udf success");
    return true;
}

void UdfModelR::mergeUdfs(std::vector<iquan::FunctionModel> &destUdfModels,
                          std::vector<iquan::FunctionModel> &srcUdfModels) {
    for (auto &srcUdfModel : srcUdfModels) {
        if (std::find(destUdfModels.begin(), destUdfModels.end(), srcUdfModel)
            == destUdfModels.end()) {
            destUdfModels.emplace_back(srcUdfModel);
        }
    }
    srcUdfModels.clear();
}

bool UdfModelR::addFunctions(iquan::CatalogDefs &catalogDefs,
                             const std::vector<std::string> &catalogList,
                             const std::vector<iquan::FunctionModel> &functionModels,
                             const std::string &dbName) {
    for (const std::string &catalogName : catalogList) {
        auto &database = catalogDefs.catalog(catalogName).database(dbName);
        for (const auto &func : functionModels) {
            if (!database.addFunction(func)) {
                SQL_LOG(ERROR,
                        "add function model [%s] failed, catalog def [%s], database [%s]",
                        func.functionName.c_str(),
                        catalogName.c_str(),
                        dbName.c_str());
                return false;
            }
        }
    }
    return true;
}

REGISTER_RESOURCE(UdfModelR);

} // namespace sql
