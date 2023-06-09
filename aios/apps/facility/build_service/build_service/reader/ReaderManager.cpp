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
#include "build_service/reader/ReaderManager.h"

#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/reader/ReaderModuleFactory.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::plugin;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, ReaderManager);

const std::string MODULE_FUNC_READER = "_Reader";

ReaderManager::ReaderManager(const ResourceReaderPtr& resourceReaderPtr) : _resourceReaderPtr(resourceReaderPtr) {}

ReaderManager::~ReaderManager() {}

bool ReaderManager::init(const ReaderConfig& readerConfig)
{
    _readerConfig = readerConfig;
    return true;
}

ReaderModuleFactory* ReaderManager::getModuleFactory(const ModuleInfo& moduleInfo)
{
    const string& moduleName = moduleInfo.moduleName;
    const PlugInManagerPtr& plugInManagerPtr = getPlugInManager(moduleInfo);
    if (!plugInManagerPtr) {
        string errorMsg = "get plugin manager failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    Module* module = _plugInManagerPtr->getModule(moduleInfo.moduleName);
    if (module == NULL) {
        string errorMsg = "GetModule [" + moduleName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    ReaderModuleFactory* factory = dynamic_cast<ReaderModuleFactory*>(module->getModuleFactory());
    if (!factory) {
        string errorMsg = "Get ReaderModuleFactory failed, moduleName:" + moduleName;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    return factory;
}

RawDocumentReader* ReaderManager::getRawDocumentReaderByModule(const ModuleInfo& moduleInfo)
{
    const string& moduleName = moduleInfo.moduleName;
    ReaderModuleFactory* factory = getModuleFactory(moduleInfo);
    if (!factory) {
        string errorMsg = "get ReaderModuleFactory failed : " + moduleName;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    RawDocumentReader* reader = factory->createRawDocumentReader(moduleName);
    if (!reader) {
        string errorMsg = "create reader failed, reader type : " + moduleName;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    return reader;
}

const plugin::PlugInManagerPtr ReaderManager::getPlugInManager(const ModuleInfo& moduleInfo)
{
    if (_plugInManagerPtr) {
        return _plugInManagerPtr;
    }
    _plugInManagerPtr = PlugInManagerPtr(
        new PlugInManager(_resourceReaderPtr, _resourceReaderPtr->getPluginPath(), MODULE_FUNC_READER));
    ModuleInfos moduleInfos;
    moduleInfos.push_back(moduleInfo);
    if (!_plugInManagerPtr->addModules(moduleInfos)) {
        string errorMsg = "Failed to create PlugInManager";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return PlugInManagerPtr(); // empty
    }
    return _plugInManagerPtr;
}

}} // namespace build_service::reader
