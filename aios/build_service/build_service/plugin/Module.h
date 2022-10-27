#ifndef ISEARCH_BS_MODULE_H
#define ISEARCH_BS_MODULE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/plugin/DllWrapper.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace config {
class ResourceReader;
}
}

namespace build_service {
namespace plugin {

class Module
{
public:
    static const std::string CREATE_FACTORY_FUNCTION_NAME;
    static const std::string DESTROY_FACTORY_FUNCTION_NAME;
public:
    Module(const std::string &path, const std::string &root,
           const std::string &moduleFuncSuffix = "");
    ~Module();
public:
    typedef ModuleFactory* (*CreateFactoryFun)();
    typedef void (*DestroyFactoryFun)(ModuleFactory *factory);
public:
    bool init(const KeyValueMap &parameters,
              const std::tr1::shared_ptr<config::ResourceReader> &resourceReaderPtr);
    void destroy();
    ModuleFactory* getModuleFactory();
    const std::string& getErrorMsg() const { return _errorMsg; }
private:
    bool doInit(const KeyValueMap &parameters,
                const std::tr1::shared_ptr<config::ResourceReader> &resourceReaderPtr);
    bool initModuleFunctions(const std::string &moduleFuncSuffix = "");
    bool getDlOpenMode(const KeyValueMap& parameters, int& mode);

 public:
    static bool parseDlOpenMode(const std::string &openModeStr, int &mode, std::string &error);

 private:
    static bool appendOpenMode(const std::string& modeStr, int& mode);

private:
    std::string _moduleRootDir;
    std::string _moduleFileName;
    std::string _moduleFuncSuffix;
    plugin::DllWrapper _dllWrapper;

    ModuleFactory *_moduleFactory;
    CreateFactoryFun _createFactoryFun;
    DestroyFactoryFun _destroyFactoryFun;

    std::string _errorMsg;
    std::mutex _mu;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_MODULE_H
