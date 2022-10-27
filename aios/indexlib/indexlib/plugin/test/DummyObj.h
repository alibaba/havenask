#ifndef __INDEXLIB_PLUGIN_DUMMYOBJ_H
#define __INDEXLIB_PLUGIN_DUMMYOBJ_H

#include "indexlib/plugin/ModuleFactory.h"

IE_NAMESPACE_BEGIN(plugin);
class DummyObj
{
public:
    virtual ~DummyObj() {}
public:
    virtual std::string getName() = 0;
};

class DummyObjFactory : public plugin::ModuleFactory
{
public:
    virtual ~DummyObjFactory() {}
    virtual DummyObj* createObj(const std::string &name) = 0;
};

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_PLUGIN_DUMMYOBJ_H
