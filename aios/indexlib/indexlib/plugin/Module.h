#ifndef __INDEXLIB_PLUGIN_MODULE_H
#define __INDEXLIB_PLUGIN_MODULE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/plugin/DllWrapper.h"
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(plugin);
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
    typedef void (*DestroyFactoryFunc)(ModuleFactory *factory); 
    
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
    Module(const std::string &path, const std::string &root);
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

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_PLUGIN_MODULE_H
