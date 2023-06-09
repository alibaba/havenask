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

#include <stddef.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/plugin/ModuleFactory.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"

namespace isearch {
namespace search {
class Optimizer;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace search {

struct OptimizerInitParam {
    OptimizerInitParam()
        : _reader(NULL)
    {}
    OptimizerInitParam(config::ResourceReader* reader, const KeyValueMap& kvMap)
        : _reader(reader)
        , _kvMap(kvMap)
    {}

    config::ResourceReader *_reader;
    KeyValueMap _kvMap;
};

static const std::string MODULE_FUNC_OPTIMIZER = "_Optimizer";
class OptimizerModuleFactory : public build_service::plugin::ModuleFactory
{
public:
    OptimizerModuleFactory() {}
    virtual ~OptimizerModuleFactory() {}
private:
    OptimizerModuleFactory(const OptimizerModuleFactory &);
    OptimizerModuleFactory& operator=(const OptimizerModuleFactory &);
public:
    virtual Optimizer* createOptimizer(const std::string &optimizerName, const OptimizerInitParam &param) = 0;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OptimizerModuleFactory> OptimizerModuleFactoryPtr;

} // namespace search
} // namespace isearch

