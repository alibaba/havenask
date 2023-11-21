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
#include "sql/ops/tvf/TvfFuncFactoryR.h"

#include <utility>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"

namespace iquan {
class TvfModels;
} // namespace iquan
namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {
class TvfFunc;

const std::string TvfFuncFactoryR::RESOURCE_ID = "sql_tvf_func_factory_r";
const std::string TvfFuncFactoryR::DYNAMIC_GROUP = "func";

TvfFuncFactoryR::TvfFuncFactoryR() {}

TvfFuncFactoryR::~TvfFuncFactoryR() {}

void TvfFuncFactoryR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ)
        .dynamicResourceGroup(DYNAMIC_GROUP, BIND_DYNAMIC_RESOURCE_TO(_tvfFuncCreators));
}

bool TvfFuncFactoryR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode TvfFuncFactoryR::init(navi::ResourceInitContext &ctx) {
    if (!initFuncCreator()) {
        return navi::EC_ABORT;
    }
    if (!initTvfInfo()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool TvfFuncFactoryR::initFuncCreator() {
    for (const auto &creatorR : _tvfFuncCreators) {
        auto funcName = creatorR->getResourceName();
        SQL_LOG(INFO, "try to add tvf function[%s].", funcName.c_str());
        if (_funcToCreator.find(funcName) != _funcToCreator.end()) {
            SQL_LOG(
                ERROR, "init tvf function failed: conflict function name[%s].", funcName.c_str());
            return false;
        }
        _funcToCreator[funcName] = creatorR;
    }
    return true;
}

bool TvfFuncFactoryR::initTvfInfo() {
    for (auto iter : _funcToCreator) {
        auto creator = iter.second;
        const auto &profileMap = creator->getSqlTvfProfileInfos();
        for (const auto &profile : profileMap) {
            const auto &info = profile.second;
            if (info.empty()) {
                continue;
            }
            const std::string &tvfName = info.tvfName;
            if (_tvfNameToCreator.find(tvfName) != _tvfNameToCreator.end()) {
                SQL_LOG(ERROR, "add tvf function failed: conflict tvf name[%s].", tvfName.c_str());
                return false;
            }
            _tvfNameToCreator[tvfName] = creator;
        }
    }
    return true;
}

bool TvfFuncFactoryR::fillTvfModels(std::vector<iquan::FunctionModel> &tvfModels) {
    for (auto iter : _funcToCreator) {
        SQL_LOG(INFO, "try to register tvf models [%s]", iter.first.c_str());
        if (!iter.second->regTvfModels(tvfModels)) {
            SQL_LOG(ERROR, "register tvf models [%s] failed", iter.first.c_str());
            return false;
        }
    }
    return true;
}

TvfFunc *TvfFuncFactoryR::createTvfFunction(const std::string &tvfName) const {
    auto it = _tvfNameToCreator.find(tvfName);
    if (it == _tvfNameToCreator.end()) {
        SQL_LOG(WARN, "tvf [%s]'s creator not found.", tvfName.c_str());
        return nullptr;
    }
    TvfFunc *tvfFunc = it->second->createFunction(tvfName);
    return tvfFunc;
}

REGISTER_RESOURCE(TvfFuncFactoryR);

} // namespace sql
