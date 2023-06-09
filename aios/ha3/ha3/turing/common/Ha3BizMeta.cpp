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
#include "ha3/turing/common/Ha3BizMeta.h"

#include <algorithm>
#include <utility>

#include "alog/Logger.h"
#include "ha3/common/CommonDef.h"
#include "ha3/turing/common/Ha3FunctionModelConverter.h"
#include "ha3/turing/common/ModelMetadata.h"
#include "ha3/turing/common/SqlConfigMetadata.h"
#include "ha3/turing/common/TableMetadata.h"
#include "ha3/turing/common/TvfMetadata.h"
#include "ha3/turing/common/UdfMetadata.h"
#include "ha3/util/TypeDefine.h"
#include "suez/turing/search/base/UserMetadata.h"

namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(ha3, Ha3BizMeta);

bool Ha3BizMeta::add(const suez::turing::UserMetadata *userMetadata) {
    for (auto metadata : userMetadata->getMetadatas()) {
        const std::string &metaType = metadata->getMetaType();
        TableMetadata *tableMetadata = dynamic_cast<TableMetadata *>(metadata);
        if (tableMetadata) {
            _sqlTableModels.merge(tableMetadata->getSqlTableModels());
            continue;
        }

        UdfMetadata *udfMetadata = dynamic_cast<UdfMetadata *>(metadata);
        if (udfMetadata) {
            _functionModels.merge(udfMetadata->getFunctionModels());
            auto status = Ha3FunctionModelConverter::convert(_functionModels);
            if (!status.ok()) {
                AUTIL_LOG(ERROR, "ha3 function model converter failed.");
                return false;
            }

            _tvfModels.merge(udfMetadata->getTvfModels());
            continue;
        }
        SqlConfigMetadata *sqlMetadata = dynamic_cast<SqlConfigMetadata *>(metadata);
        if (sqlMetadata) {
            _sqlConfigPtr = sqlMetadata->getSqlConfigPtr();
            if (_sqlConfigPtr != nullptr && !_sqlConfigPtr->sqlConfig.remoteTables.empty()) {
                AUTIL_LOG(INFO, "merge remote table size [%zu]", _sqlConfigPtr->sqlConfig.remoteTables.tables.size());
                _sqlTableModels.merge(_sqlConfigPtr->sqlConfig.remoteTables);
            }
            continue;
        }
        ModelMetadata *modelMetadata = dynamic_cast<ModelMetadata *>(metadata);
        if (modelMetadata) {
            const ModelConfig &modelConfig = modelMetadata->getModelConfig();
            auto ret = _modelConfigMap.insert({modelConfig.bizName, modelConfig});
            if (ret.second == false) {
                AUTIL_LOG(ERROR, "redefine model biz [%s]", modelConfig.bizName.c_str());
                return false;
            }
            continue;
        }
        TvfMetadata *tvfMetadata = dynamic_cast<TvfMetadata *>(metadata);
        if (tvfMetadata) {
            const auto &tvfNameToCreator = tvfMetadata->getTvfNameToCreator();
            _tvfNameToCreator.insert(tvfNameToCreator.begin(), tvfNameToCreator.end());
            continue;
        }
        AUTIL_LOG(WARN, "metadata not support [%s] type", metaType.c_str());
    }
    return true;
}

std::unique_ptr<iquan::CatalogInfo> Ha3BizMeta::generatorCatalogInfo() const {
    auto catalogInfo = std::make_unique<iquan::CatalogInfo>();
    catalogInfo->computeNodeModels = {};
    catalogInfo->tableModels = _sqlTableModels;
    catalogInfo->functionModels = _functionModels;
    catalogInfo->tvfFunctionModels = _tvfModels;
    return catalogInfo;
}

} // namespace turing
} // namespace isearch
