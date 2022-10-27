#ifndef ISEARCH_BS_PLUGINMANAGER_H
#define ISEARCH_BS_PLUGINMANAGER_H

#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleInfo.h"
#include <mutex>

namespace build_service {
namespace config {
class ResourceReader;
}
}

namespace build_service {
namespace plugin {

class Module;

const std::string MODULE_FUNC_BUILDER = "_Builder";
const std::string MODULE_FUNC_TOKENIZER = "_Tokenizer";
const std::string MODULE_FUNC_MERGER = "_Merger";
const std::string MODULE_FUNC_TASK = "_Task";

class PlugInManager
{
public:
    PlugInManager(const std::tr1::shared_ptr<config::ResourceReader> &resourceReaderPtr,
                  const std::string& pluginPath,
                  const std::string &moduleFuncSuffix);
    PlugInManager(const std::tr1::shared_ptr<config::ResourceReader> &resourceReaderPtr,
                  const std::string &moduleFuncSuffix = "");
    ~PlugInManager();

public:
    bool addModules(const ModuleInfos &moduleInfos);

    Module* getModule(const std::string &moduleName);
    // get default module
    Module* getModule();

    bool addModule(const ModuleInfo &moduleInfo);

public:
    static bool isBuildInModule(const std::string &moduleName) {
        return moduleName.empty() || moduleName == "BuildInModule";
    }

private:
    ModuleInfo getModuleInfo(const std::string &moduleName);
    void clear();
    std::string getPluginPath(const std::string& moduleFileName);
    bool parseSoUnderOnePath(
			     const std::string& path, const std::string& moduleFileName);
private:
    std::map<std::string, Module*> _name2moduleMap;
    std::map<std::string, ModuleInfo> _name2moduleInfoMap;
    const std::string _moduleFuncSuffix;
    std::tr1::shared_ptr<config::ResourceReader> _resourceReaderPtr;
    const std::string _pluginPath;
    std::mutex _mu;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PlugInManager);

}
}

#endif //ISEARCH_BS_PLUGINMANAGER_H
