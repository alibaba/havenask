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
#include "build_service/custom_merger/CustomMergerCreator.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "build_service/config/ProcessorInfo.h"
#include "build_service/custom_merger/BuildinCustomMergerFactory.h"
#include "build_service/custom_merger/CustomMergerFactory.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"

using namespace std;
using namespace build_service::plugin;
using namespace build_service::config;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergerCreator);

CustomMergerCreator::CustomMergerCreator() : _metricProvider(NULL) {}

CustomMergerCreator::~CustomMergerCreator() {}

CustomMergerPtr CustomMergerCreator::create(const config::MergePluginConfig& mergeConfig,
                                            MergeResourceProviderPtr& resourceProvider, uint32_t backupId,
                                            const std::string& epochId)
{
    _pluginManager.reset(
        new PlugInManager(_resourceReaderPtr, _resourceReaderPtr->getPluginPath(), MODULE_FUNC_MERGER));
    if (!_pluginManager->addModule(mergeConfig.getModuleInfo())) {
        return CustomMergerPtr();
    }

    BuildinCustomMergerFactory buildInFactory;
    CustomMergerFactory* factory = NULL;
    const ProcessorInfo& processorInfo = mergeConfig.getProcessorInfo();

    factory = &buildInFactory;
    string moduleName = processorInfo.moduleName;
    if (PlugInManager::isBuildInModule(moduleName)) {
        factory = &buildInFactory;
    } else {
        Module* module = _pluginManager->getModule(moduleName);
        if (!module) {
            string errorMsg = "Init custom merger failed. no module name[" + moduleName + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return CustomMergerPtr();
        }
        factory = dynamic_cast<CustomMergerFactory*>(module->getModuleFactory());
        if (!factory) {
            string errorMsg = "Invalid module[" + moduleName + "].";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return CustomMergerPtr();
        }
    }
    CustomMerger::CustomMergerInitParam param;
    param.resourceProvider = resourceProvider;
    param.parameters = processorInfo.parameters;
    param.metricProvider = _metricProvider;
    param.resourceReader = _resourceReaderPtr.get();

    CustomMergerPtr customMerger(factory->createCustomMerger(processorInfo.className, backupId, epochId));
    if (!customMerger) {
        string errorMsg = "create custom merger[" + processorInfo.className + "] from factory failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return CustomMergerPtr();
    }

    if (!customMerger->init(param)) {
        string errorMsg = "Init custom merger [" + processorInfo.className + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return CustomMergerPtr();
    }
    return customMerger;
}

CustomMerger::CustomMergerInitParam CustomMergerCreator::getTestParam()
{
    CustomMerger::CustomMergerInitParam param; // add index provider
    KeyValueMap parameters;
    // param.resourceProvider = resourceProvider;
    param.parameters = parameters;
    return param;
}

}} // namespace build_service::custom_merger
