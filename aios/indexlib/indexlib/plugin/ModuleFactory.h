#ifndef __INDEXLIB_PLUGIN_MODULEFACTORY_H
#define __INDEXLIB_PLUGIN_MODULEFACTORY_H

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"

IE_NAMESPACE_BEGIN(plugin);
class ModuleFactory
{
public:
    ModuleFactory() {}
    virtual ~ModuleFactory() {}
public:
    virtual bool init()
    {
        return true;
    }
    virtual void destroy() = 0;
};

extern "C" ModuleFactory* createFactory();
extern "C" void destroyFactory(ModuleFactory *factory);

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_PLUGIN_MODULEFACTORY_H
