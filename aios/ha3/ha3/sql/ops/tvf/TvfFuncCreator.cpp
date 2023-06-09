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
#include "ha3/sql/ops/tvf/TvfFuncCreator.h"

#include <algorithm>
#include <iosfwd>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/TvfFunctionDef.h"
#include "iquan/common/catalog/TvfFunctionModel.h"

using namespace std;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TvfFuncCreator);

bool TvfFuncCreator::regTvfModels(iquan::TvfModels &tvfModels) {
    for (auto pair : _sqlTvfProfileInfos) {
        iquan::TvfModel tvfModel;
        if (!generateTvfModel(tvfModel, pair.second)) {
            SQL_LOG(ERROR, "generate tvf model [%s] failed", pair.first.c_str());
            return false;
        }
        if (!initTvfModel(tvfModel, pair.second)) {
            SQL_LOG(ERROR, "init tvf model [%s] failed", pair.first.c_str());
            return false;
        }
        if (!checkTvfModel(tvfModel)) {
            SQL_LOG(ERROR, "tvf model [%s] invalid", pair.first.c_str());
            return false;
        }
        tvfModels.functions.push_back(tvfModel);
    }
    return true;
}

bool TvfFuncCreator::generateTvfModel(iquan::TvfModel &tvfModel, const SqlTvfProfileInfo &info) {
    try {
        FromJsonString(tvfModel, _tvfDef);
    } catch (...) {
        SQL_LOG(ERROR, "register tvf models failed [%s].", _tvfDef.c_str());
        return false;
    }
    tvfModel.functionName = info.tvfName;
    return true;
}

bool TvfFuncCreator::checkTvfModel(const iquan::TvfModel &tvfModel) {
    if (tvfModel.functionContent.tvfs.size() != 1) {
        SQL_LOG(ERROR, "tvf [%s] func prototypes not equal 1.", tvfModel.functionName.c_str());
        return false;
    }
    const auto &tvfDef = tvfModel.functionContent.tvfs[0];
    for (const auto &paramType : tvfDef.params.scalars) {
        if (paramType.type != "string") {
            SQL_LOG(ERROR,
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
