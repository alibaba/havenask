#ifndef ISEARCH_BS_NAMEDFACTORYMODULE_H
#define ISEARCH_BS_NAMEDFACTORYMODULE_H

#include "indexlib/common_define.h"

#include "indexlib/plugin/ModuleFactory.h"

IE_NAMESPACE_BEGIN(plugin);

class NamedFactoryModule : public ModuleFactory {
public:
    NamedFactoryModule() {}
    ~NamedFactoryModule() {}
public:
    virtual const std::string &getSuffix() const  = 0;
};

DEFINE_SHARED_PTR(NamedFactoryModule);

IE_NAMESPACE_END(plugin);

#endif //ISEARCH_BS_NAMEDFACTORYMODULE_H
