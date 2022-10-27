#ifndef ISEARCH_BS_READERMANAGER_H
#define ISEARCH_BS_READERMANAGER_H

#include "build_service/util/Log.h"
#include "build_service/reader/ReaderConfig.h"

namespace build_service {
namespace reader {
class RawDocumentReader;
class ReaderModuleFactory;
}
}

namespace build_service {
namespace plugin {
class PlugInManager;
}
}

namespace build_service {
namespace config {
class ResourceReader;
}
}

namespace build_service {
namespace reader {

class ReaderManager
{
public:
    ReaderManager(const std::tr1::shared_ptr<config::ResourceReader> &resourceReaderPtr);
    virtual ~ReaderManager();
private:
    ReaderManager(const ReaderManager &);
    ReaderManager& operator=(const ReaderManager &);
public:
    bool init(const ReaderConfig &readerConfig);
    RawDocumentReader* getRawDocumentReaderByModule(const plugin::ModuleInfo &moduleInfo);

public:

protected:
    virtual ReaderModuleFactory* getModuleFactory(const plugin::ModuleInfo &moduleInfo);
    virtual const std::tr1::shared_ptr<plugin::PlugInManager> getPlugInManager(
            const plugin::ModuleInfo &moduleInfo);
private:
    std::tr1::shared_ptr<config::ResourceReader> _resourceReaderPtr;
    std::tr1::shared_ptr<plugin::PlugInManager> _plugInManagerPtr;
    ReaderConfig _readerConfig;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReaderManager);

}
}

#endif //ISEARCH_BS_READERMANAGER_H
