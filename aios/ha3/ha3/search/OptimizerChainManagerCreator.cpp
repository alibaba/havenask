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
#include "ha3/search/OptimizerChainManagerCreator.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/OptimizerConfigInfo.h"
#include "ha3/config/SearchOptimizerConfig.h"
#include "ha3/search/AuxiliaryChainOptimizer.h"
#include "ha3/search/Optimizer.h"
#include "ha3/search/OptimizerChainManager.h"
#include "ha3/search/OptimizerModuleFactory.h"

using namespace std;
using namespace build_service::plugin;
using namespace autil::legacy;
using namespace isearch::config;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, OptimizerChainManagerCreator);

OptimizerChainManagerCreator::OptimizerChainManagerCreator(
        const ResourceReaderPtr &resourceReaderPtr)
    : _resourceReaderPtr(resourceReaderPtr)
{
}

OptimizerChainManagerCreator::~OptimizerChainManagerCreator() {
}

OptimizerChainManagerPtr OptimizerChainManagerCreator::createFromString(const std::string &configStr) {
    SearchOptimizerConfig optimizerConfig;
    if(!configStr.empty()) {
        try {
            FromJsonString(optimizerConfig, configStr);
        } catch (const ExceptionBase &e) {
            AUTIL_LOG(ERROR, "Catch exception in OptimizerChainManagerCreator: [%s]\n"
                    " FromJsonString fail: [%s]", e.what(), configStr.c_str());
            return OptimizerChainManagerPtr();
        }
    }
    return create(optimizerConfig);
}

OptimizerChainManagerPtr OptimizerChainManagerCreator::create(
        const SearchOptimizerConfig &optimizerConfig)
{
    OptimizerChainManagerPtr optimizerChainManagerPtr(
            new OptimizerChainManager(_resourceReaderPtr, MODULE_FUNC_OPTIMIZER));
    addBuildInOptimizers(optimizerChainManagerPtr);

    if(!optimizerChainManagerPtr->addModules(optimizerConfig.getModuleInfos())) {
        AUTIL_LOG(ERROR, "Load optimizer module failed : %s",
                  ToJsonString(optimizerConfig.getModuleInfos()).c_str());
        return OptimizerChainManagerPtr();
    }
    const OptimizerConfigInfos &optimizerConfigInfos =
        optimizerConfig.getOptimizerConfigInfos();
    for (size_t i = 0; i != optimizerConfigInfos.size(); i ++) {
        const OptimizerConfigInfo &optimizerConfigInfo = optimizerConfigInfos[i];
        Module *module = optimizerChainManagerPtr->getModule(
                optimizerConfigInfo.moduleName);
        if(module == NULL) {
            AUTIL_LOG(ERROR, "Get module[%s] failed.", optimizerConfigInfo.moduleName.c_str());
            return OptimizerChainManagerPtr();
        }
        OptimizerModuleFactory *factory = dynamic_cast<OptimizerModuleFactory *>(module->getModuleFactory());
        if (factory == NULL) {
            AUTIL_LOG(ERROR, "Get module factory failed.");
            return OptimizerChainManagerPtr();
        }

        OptimizerInitParam param(_resourceReaderPtr.get(), optimizerConfigInfo.parameters);
        OptimizerPtr optimizerPtr(factory->createOptimizer(optimizerConfigInfo.optimizerName, param));
        if (!optimizerPtr) {
            AUTIL_LOG(ERROR, "create optimizer[%s] failed.", optimizerConfigInfo.optimizerName.c_str());
            return OptimizerChainManagerPtr();
        }
        if (!optimizerChainManagerPtr->addOptimizer(optimizerPtr)) {
            AUTIL_LOG(ERROR, "duplicate optimizer name [%s].", optimizerConfigInfo.optimizerName.c_str());
            return OptimizerChainManagerPtr();
        }
    }
    return optimizerChainManagerPtr;
}

void OptimizerChainManagerCreator::addBuildInOptimizers(
        OptimizerChainManagerPtr &optimizerChainManagerPtr)
{
    OptimizerPtr optimizerPtr(new AuxiliaryChainOptimizer());
    optimizerChainManagerPtr->addOptimizer(optimizerPtr);
}

} // namespace search
} // namespace isearch

