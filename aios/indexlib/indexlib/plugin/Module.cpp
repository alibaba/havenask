#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/util/path_util.h"
#include <dlfcn.h>
#include <cassert>

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(plugin);
IE_LOG_SETUP(plugin, Module);

IE_LOG_SETUP(plugin, ModuleFactoryCreator);

const std::string ModuleFactoryCreator::CREATE_FACTORY_FUNCTION_NAME = "create";
const std::string ModuleFactoryCreator::DESTROY_FACTORY_FUNCTION_NAME = "destroy";

ModuleFactoryCreator::ModuleFactoryCreator()
    : mModuleFactory(NULL)
    , mCreateFactoryFunc(NULL)
    , mDestroyFactoryFunc(NULL)
{   
}

ModuleFactoryCreator::~ModuleFactoryCreator()
{
    if (mModuleFactory && mDestroyFactoryFunc)
    {
        mDestroyFactoryFunc(mModuleFactory);
    }
    mModuleFactory = NULL;
    mCreateFactoryFunc = NULL;
    mDestroyFactoryFunc = NULL;
}

ModuleFactory* ModuleFactoryCreator::getFactory()
{
    if (!mModuleFactory && mCreateFactoryFunc)
    {
        mModuleFactory = mCreateFactoryFunc();
    }
    return mModuleFactory;
}

bool ModuleFactoryCreator::Init(plugin::DllWrapper& dllWrapper,
                                const std::string& factoryName)
{
    string symbolStr = CREATE_FACTORY_FUNCTION_NAME + factoryName;
    mCreateFactoryFunc = (CreateFactoryFunc)dllWrapper.dlsym(symbolStr);
    if (!mCreateFactoryFunc) {
        IE_LOG(ERROR, "fail to get symbol [%s] from module[%s]",
               symbolStr.c_str(), dllWrapper.getLocalLibPath().c_str());
        return false;
    }
    symbolStr = DESTROY_FACTORY_FUNCTION_NAME + factoryName; 
    mDestroyFactoryFunc = (DestroyFactoryFunc)dllWrapper.dlsym(
        DESTROY_FACTORY_FUNCTION_NAME + factoryName);
    if (!mDestroyFactoryFunc) {
        IE_LOG(ERROR, "fail to get symbol [%s] from module[%s]",
               symbolStr.c_str(), dllWrapper.getLocalLibPath().c_str());
        return false;
    }
    return true;
}

Module::Module(const string &path, const std::string &root)
    : _moduleRootDir(root)
    , _moduleFileName(path)
    , _dllWrapper(util::PathUtil::JoinPath(root, path))
{
}

Module::~Module() {
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
        IE_LOG(ERROR, "init ModuleFactoryCreator for factory[%s] failed",
               factoryName.c_str())
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

IE_NAMESPACE_END(plugin);

