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
#include <string>

#include "autil/Lock.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/plugin/DllWrapper.h"

namespace indexlib { namespace plugin {
class ModuleFactory;

class ModuleFactoryCreator
{
public:
    ModuleFactoryCreator();
    ~ModuleFactoryCreator();

public:
    bool Init(plugin::DllWrapper& _dllWrapper, const std::string& factoryName);
    ModuleFactory* getFactory();

public:
    static const std::string CREATE_FACTORY_FUNCTION_NAME;
    static const std::string DESTROY_FACTORY_FUNCTION_NAME;
    typedef ModuleFactory* (*CreateFactoryFunc)();
    typedef void (*DestroyFactoryFunc)(ModuleFactory* factory);

private:
    ModuleFactory* mModuleFactory;
    CreateFactoryFunc mCreateFactoryFunc;
    DestroyFactoryFunc mDestroyFactoryFunc;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ModuleFactoryCreator);

class Module
{
public:
public:
    Module(const std::string& path, const std::string& root);
    ~Module();

public:
    bool init();
    ModuleFactory* getModuleFactory(const std::string& factoryName);

private:
    bool doInit();

private:
    std::string _moduleRootDir;
    std::string _moduleFileName;
    plugin::DllWrapper _dllWrapper;
    mutable autil::ThreadMutex mMapLock;
    std::map<std::string, ModuleFactoryCreatorPtr> mFactoryCreatorMap;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::plugin
