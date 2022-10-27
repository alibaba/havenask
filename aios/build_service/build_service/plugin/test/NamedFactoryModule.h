#ifndef ISEARCH_BS_NAMEDFACTORYMODULE_H
#define ISEARCH_BS_NAMEDFACTORYMODULE_H

#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace plugin {

class NamedFactoryModule : public ModuleFactory {
public:
    NamedFactoryModule() {}
    ~NamedFactoryModule() {}
public:
    virtual const std::string &getSuffix() const  = 0;
};

BS_TYPEDEF_PTR(NamedFactoryModule);

}
}

#endif //ISEARCH_BS_NAMEDFACTORYMODULE_H
