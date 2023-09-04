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

#include "iquan/common/catalog/TableModel.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"

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
    NAVI_JSONIZE(ctx, "default_udf", _defaultUdfFunctionModels, _defaultUdfFunctionModels);
    NAVI_JSONIZE(ctx, "zone_udf_map", _zoneUdfMap, _zoneUdfMap);
    NAVI_JSONIZE(ctx, "special_catalogs", _specialCatalogs, _specialCatalogs);
    return true;
}

navi::ErrorCode UdfModelR::init(navi::ResourceInitContext &ctx) {
    for (const auto &pair : _zoneUdfMap) {
        const auto &dbName = pair.first;
        auto zoneModels = _defaultUdfFunctionModels;
        for (const auto &model : pair.second) {
            zoneModels.insert(model);
        }
        _zoneFunctionModels[dbName] = zoneModels;
    }
    return navi::EC_NONE;
}

void UdfModelR::fillZoneUdfMap(const iquan::TableModels &tableModels) {
    for (const auto &tableModel : tableModels.tables) {
        auto &dbName = tableModel.databaseName;
        auto iter = _zoneFunctionModels.find(dbName);
        if (iter == _zoneFunctionModels.end()) {
            _zoneFunctionModels[dbName] = _defaultUdfFunctionModels;
        }
    }
}

bool UdfModelR::fillFunctionModels(iquan::FunctionModels &functionModels,
                                   iquan::TvfModels &tvfModels) const {
    for (const auto &pair : _zoneFunctionModels) {
        const std::string &dbName = pair.first;
        addUserFunctionModels<iquan::FunctionModels>(
            pair.second, _specialCatalogs, dbName, functionModels);
        if (_aggFuncFactoryR) {
            addUserFunctionModels<iquan::FunctionModels>(
                _aggFuncFactoryR->getFunctionModels(), _specialCatalogs, dbName, functionModels);
        }
        if (_tvfFuncFactoryR) {
            iquan::TvfModels inputTvfModels;
            if (!_tvfFuncFactoryR->fillTvfModels(inputTvfModels)) {
                SQL_LOG(ERROR, "reg tvf models failed, db [%s]", dbName.c_str());
                return false;
            }
            addUserFunctionModels<iquan::TvfModels>(
                inputTvfModels, _specialCatalogs, dbName, tvfModels);
        }
    }
    return true;
}

REGISTER_RESOURCE(UdfModelR);

} // namespace sql
