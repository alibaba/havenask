#ifndef ISEARCH_BS_MODULEFACTORY_H
#define ISEARCH_BS_MODULEFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include <indexlib/plugin/ModuleFactory.h>

namespace build_service {
namespace plugin {

class ModuleFactory : public IE_NAMESPACE(plugin)::ModuleFactory
{
public:
    ModuleFactory() {}
    virtual ~ModuleFactory() {}
public:
    virtual bool init(const KeyValueMap &parameters) {
        return true;
    }
    virtual bool init(const KeyValueMap &parameters,
                      const config::ResourceReaderPtr &resourceReaderPtr) {
        return init(parameters);
    }
private:
    BS_LOG_DECLARE();
};

}
}

// for compatible
namespace build_service {
namespace util {
using plugin::ModuleFactory;
}
}

#endif //ISEARCH_BS_MODULEFACTORY_H
