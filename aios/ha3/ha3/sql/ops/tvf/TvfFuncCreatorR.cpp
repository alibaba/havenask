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
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"

#include <common.h>
#include <engine/ResourceConfigContext.h>
#include <algorithm>
#include <utility>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/TvfFunctionDef.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "navi/log/NaviLogger.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace isearch {
namespace sql {

bool TvfFuncCreatorR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode TvfFuncCreatorR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

TvfFunc *TvfFuncCreatorR::createFunction(const std::string &name) {
    auto iter = _sqlTvfProfileInfos.find(name);
    if (iter == _sqlTvfProfileInfos.end()) {
        NAVI_KERNEL_LOG(WARN, "tvf [%s]'s func info not found.", name.c_str());
        return nullptr;
    }
    TvfFunc *tvfFunc = createFunction(iter->second);
    if (tvfFunc != nullptr) {
        tvfFunc->setName(name);
    }
    return tvfFunc;
}

std::string TvfFuncCreatorR::getName() {
    return _name;
}

void TvfFuncCreatorR::setName(const std::string &name) {
    _name = name;
}

void TvfFuncCreatorR::addTvfFunction(const SqlTvfProfileInfo &sqlTvfProfileInfo) {
    NAVI_KERNEL_LOG(INFO, "add tvf function info [%s]", sqlTvfProfileInfo.tvfName.c_str());
    _sqlTvfProfileInfos[sqlTvfProfileInfo.tvfName] = sqlTvfProfileInfo;
}

SqlTvfProfileInfo TvfFuncCreatorR::getDefaultTvfInfo() {
    if (_sqlTvfProfileInfos.size() == 1) {
        return _sqlTvfProfileInfos.begin()->second;
    }
    return {};
}

bool TvfFuncCreatorR::regTvfModels(iquan::TvfModels &tvfModels) {
    for (auto pair : _sqlTvfProfileInfos) {
        iquan::TvfModel tvfModel;
        if (!generateTvfModel(tvfModel, pair.second)) {
            NAVI_KERNEL_LOG(ERROR, "generate tvf model [%s] failed", pair.first.c_str());
            return false;
        }
        if (!initTvfModel(tvfModel, pair.second)) {
            NAVI_KERNEL_LOG(ERROR, "init tvf model [%s] failed", pair.first.c_str());
            return false;
        }
        if (!checkTvfModel(tvfModel)) {
            NAVI_KERNEL_LOG(ERROR, "tvf model [%s] invalid", pair.first.c_str());
            return false;
        }
        tvfModels.functions.push_back(tvfModel);
    }
    return true;
}

bool TvfFuncCreatorR::generateTvfModel(iquan::TvfModel &tvfModel, const SqlTvfProfileInfo &info) {
    try {
        FromJsonString(tvfModel, _tvfDef);
    } catch (...) {
        NAVI_KERNEL_LOG(ERROR, "register tvf models failed [%s].", _tvfDef.c_str());
        return false;
    }
    tvfModel.functionName = info.tvfName;
    return true;
}

bool TvfFuncCreatorR::checkTvfModel(const iquan::TvfModel &tvfModel) {
    if (tvfModel.functionContent.tvfs.size() != 1) {
        NAVI_KERNEL_LOG(
            ERROR, "tvf [%s] func prototypes not equal 1.", tvfModel.functionName.c_str());
        return false;
    }
    const auto &tvfDef = tvfModel.functionContent.tvfs[0];
    for (const auto &paramType : tvfDef.params.scalars) {
        if (paramType.type != "string") {
            NAVI_KERNEL_LOG(ERROR,
                            "tvf [%s] func prototypes only support string type. now is [%s]",
                            tvfModel.functionName.c_str(),
                            paramType.type.c_str());
            return false;
        }
    }
    return true;
}

} // namespace sql
} // namespace isearch
