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
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/plugin/InterfaceManager.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/config/ResourceReader.h"
#include "ha3/sql/ops/agg/AggFuncMode.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "table/DataCommon.h"

namespace isearch {
namespace sql {
class AggFunc;
class AggFuncCreatorFactory;
class AggFuncCreatorR;
class SqlAggPluginConfig;

typedef std::shared_ptr<AggFuncCreatorFactory> AggFuncCreatorFactoryPtr;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class AggFuncManager : public build_service::plugin::PlugInManager
{
public:
    AggFuncManager(const build_service::config::ResourceReaderPtr &resourceReader);
    ~AggFuncManager();

private:
    AggFuncManager(const AggFuncManager &);
    AggFuncManager &operator=(const AggFuncManager &);

public:
    bool init(const SqlAggPluginConfig &aggPluginConfig);
    AggFunc *createAggFunction(const std::string &aggFuncName,
                               const std::vector<table::ValueType> &inputTypes,
                               const std::vector<std::string> &inputFields,
                               const std::vector<std::string> &outputFields,
                               AggFuncMode mode) const;
    bool getAccSize(const std::string &name, size_t &size) const {
        auto iter = _accSize.find(name);
        if (iter == _accSize.end()) {
            return false;
        }
        size = iter->second;
        return true;
    }
    iquan::FunctionModels getFunctionModels() const {
        return _functionModels;
    }

private:
    bool initBuiltinFactory();
    bool initPluginFactory(const SqlAggPluginConfig &aggPluginConfig);
    bool registerFactoryFunctions(AggFuncCreatorFactory *factory);

private:
    std::unique_ptr<AggFuncCreatorFactory> _builtinFactory;
    std::map<std::string, size_t> _accSize;
    iquan::FunctionModels _functionModels;
    autil::InterfaceManager _interfaceManager;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggFuncManager> AggFuncManagerPtr;
} // namespace sql
} // namespace isearch
