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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/SqlConfig.h"
#include "ha3/turing/common/ModelConfig.h"
#include "iquan/common/catalog/CatalogInfo.h"
#include "suez/turing/search/base/UserMetadata.h"

namespace suez {
namespace turing {
class UserMetadata;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace sql {
class TvfFuncCreatorR;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace turing {

class Ha3BizMeta {
public:
    bool add(const suez::turing::UserMetadata *userMetadata);
    isearch::config::SqlConfigPtr getSqlConfig() const {
        return _sqlConfigPtr;
    }
    const iquan::TableModels &getSqlTableModels() const {
        return _sqlTableModels;
    }
    const iquan::FunctionModels &getFunctionModels() const {
        return _functionModels;
    }
    const iquan::TvfModels &getTvfModels() const{
        return _tvfModels;
    }
    ModelConfigMap *getModelConfigMap() {
        return &_modelConfigMap;
    }
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> *getTvfNameToCreator() {
        return &_tvfNameToCreator;
    }

    std::unique_ptr<iquan::CatalogInfo> generatorCatalogInfo() const;
private:
    isearch::config::SqlConfigPtr _sqlConfigPtr;
    ModelConfigMap _modelConfigMap;
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> _tvfNameToCreator;

    // TODO remove, replaced by serviceSnapshot._catalogInfo
    iquan::TableModels _sqlTableModels;
    iquan::FunctionModels _functionModels;
    iquan::TvfModels _tvfModels;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<Ha3BizMeta> Ha3BizMetaPtr;

} // namespace turing
} // namespace isearch
