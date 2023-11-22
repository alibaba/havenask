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
#include "indexlib/plugin/Module.h"

#include <cstddef>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace plugin {
IE_LOG_SETUP(plugin, Module);

IE_LOG_SETUP(plugin, ModuleFactoryCreator);

const std::string ModuleFactoryCreator::CREATE_FACTORY_FUNCTION_NAME = "create";
const std::string ModuleFactoryCreator::DESTROY_FACTORY_FUNCTION_NAME = "destroy";

ModuleFactoryCreator::ModuleFactoryCreator() : mModuleFactory(NULL), mCreateFactoryFunc(NULL), mDestroyFactoryFunc(NULL)
{
}

ModuleFactoryCreator::~ModuleFactoryCreator()
{
    if (mModuleFactory && mDestroyFactoryFunc) {
        mDestroyFactoryFunc(mModuleFactory);
    }
    mModuleFactory = NULL;
    mCreateFactoryFunc = NULL;
    mDestroyFactoryFunc = NULL;
}

ModuleFactory* ModuleFactoryCreator::getFactory()
{
    if (!mModuleFactory && mCreateFactoryFunc) {
        mModuleFactory = mCreateFactoryFunc();
    }
    return mModuleFactory;
}

bool ModuleFactoryCreator::Init(plugin::DllWrapper& dllWrapper, const std::string& factoryName)
{
    string symbolStr = CREATE_FACTORY_FUNCTION_NAME + factoryName;
    mCreateFactoryFunc = (CreateFactoryFunc)dllWrapper.dlsym(symbolStr);
    if (!mCreateFactoryFunc) {
        IE_LOG(ERROR, "fail to get symbol [%s] from module[%s]", symbolStr.c_str(),
               dllWrapper.getLocalLibPath().c_str());
        return false;
    }
    symbolStr = DESTROY_FACTORY_FUNCTION_NAME + factoryName;
    mDestroyFactoryFunc = (DestroyFactoryFunc)dllWrapper.dlsym(DESTROY_FACTORY_FUNCTION_NAME + factoryName);
    if (!mDestroyFactoryFunc) {
        IE_LOG(ERROR, "fail to get symbol [%s] from module[%s]", symbolStr.c_str(),
               dllWrapper.getLocalLibPath().c_str());
        return false;
    }
    return true;
}

Module::Module(const string& path, const std::string& root)
    : _moduleRootDir(root)
    , _moduleFileName(path)
    , _dllWrapper(util::PathUtil::JoinPath(root, path))
{
}

Module::~Module()
{
    autil::ScopedLock lock(mMapLock);
    mFactoryCreatorMap.clear();
}

bool Module::init()
{
    bool ret = doInit();
    if (!ret) {
        return false;
    }
    return ret;
}

ModuleFactory* Module::getModuleFactory(const string& factoryName)
{
    autil::ScopedLock lock(mMapLock);
    auto it = mFactoryCreatorMap.find(factoryName);

    if (it != mFactoryCreatorMap.end()) {
        return it->second->getFactory();
    }

    ModuleFactoryCreatorPtr creator(new ModuleFactoryCreator());
    if (!creator->Init(_dllWrapper, factoryName)) {
        IE_LOG(ERROR, "init ModuleFactoryCreator for factory[%s] failed", factoryName.c_str())
        return NULL;
    }
    mFactoryCreatorMap.insert(make_pair(factoryName, creator));
    return creator->getFactory();
}

bool Module::doInit()
{
    if (!_dllWrapper.dlopen()) {
        IE_LOG(ERROR, "open dll fail:[%s]", _moduleFileName.c_str());
        return false;
    }
    return true;
}

}} // namespace indexlib::plugin
