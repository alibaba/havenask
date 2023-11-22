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
#include <mutex>
#include <string>

#include "build_service/common_define.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {
class ResourceReader;
}} // namespace build_service::config

namespace build_service { namespace plugin {

class Module;

constexpr char MODULE_FUNC_BUILDER[] = "_Builder";
constexpr char MODULE_FUNC_TOKENIZER[] = "_Tokenizer";
constexpr char MODULE_FUNC_MERGER[] = "_Merger";
constexpr char MODULE_FUNC_TASK[] = "_Task";

class PlugInManager
{
public:
    PlugInManager(const std::shared_ptr<config::ResourceReader>& resourceReaderPtr, const std::string& pluginPath,
                  const std::string& moduleFuncSuffix);
    PlugInManager(const std::shared_ptr<config::ResourceReader>& resourceReaderPtr,
                  const std::string& moduleFuncSuffix = "");
    virtual ~PlugInManager();

public:
    bool addModules(const ModuleInfos& moduleInfos);

    Module* getModule(const std::string& moduleName);
    // get default module
    Module* getModule();

    bool addModule(const ModuleInfo& moduleInfo);

public:
    static bool isBuildInModule(const std::string& moduleName)
    {
        return moduleName.empty() || moduleName == "BuildInModule";
    }

private:
    ModuleInfo getModuleInfo(const std::string& moduleName);
    void clear();
    std::string getPluginPath(const std::string& moduleFileName);
    bool parseSoUnderOnePath(const std::string& path, const std::string& moduleFileName);

protected:
    std::map<std::string, Module*> _name2moduleMap;
    std::map<std::string, ModuleInfo> _name2moduleInfoMap;
    const std::string _moduleFuncSuffix;
    std::shared_ptr<config::ResourceReader> _resourceReaderPtr;
    const std::string _pluginPath;
    std::mutex _mu;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PlugInManager);

}} // namespace build_service::plugin
