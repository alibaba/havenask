#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <build_service/plugin/Module.h>

using namespace std;
using namespace build_service::plugin;
using namespace build_service::config;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, AggFuncManager);

AggFuncManager::AggFuncManager() {
}

AggFuncManager::~AggFuncManager() {
    for (auto it : _pluginsFactory) {
        delete it.second;
    }
}

bool AggFuncManager::init(const std::string &configPath,
                          const HA3_NS(config)::SqlAggPluginConfig &aggPluginConfig)
{
    if (!initBuiltinFactory()) {
        return false;
    }
    if (!initPluginFactory(configPath, aggPluginConfig)) {
        return false;
    }
    return true;
}

bool AggFuncManager::initBuiltinFactory() {
    if (!_builtinFactory.registerAggFunctions()) {
        SQL_LOG(ERROR, "register builtinAggFuncCreatorFactory failed.");
        return false;
    }
    if (!addFunctions(&_builtinFactory)) {
        SQL_LOG(ERROR, "add builtin agg functions failed.");
        return false;
    }
    return true;
}

bool AggFuncManager::initPluginFactory(const std::string &configPath,
                                       const SqlAggPluginConfig &aggPluginConfig)
{
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configPath));
    _pluginManagerPtr.reset(new PlugInManager(resourceReaderPtr, MODULE_SQL_AGG_FUNCTION));
    if (!_pluginManagerPtr->addModules(aggPluginConfig.modules)) {
        SQL_LOG(ERROR, "load sql agg function modules failed : %s.",
                  ToJsonString(aggPluginConfig.modules).c_str());
        return false;
    }
    auto &modules = aggPluginConfig.modules;
    for (size_t i = 0; i < modules.size(); ++i) {
        const string &moduleName = modules[i].moduleName;
        Module *module = _pluginManagerPtr->getModule(moduleName);
        if (module == nullptr) {
            SQL_LOG(ERROR, "Get module [%s] failed", moduleName.c_str());
            return false;
        }
        AggFuncCreatorFactory *factory = dynamic_cast<AggFuncCreatorFactory *>(
                module->getModuleFactory());
        if (factory == nullptr) {
            SQL_LOG(ERROR, "invalid AggFuncCreatorFactory in module[%s].",
                      moduleName.c_str());
            return false;
        }
        _pluginsFactory.push_back(make_pair(moduleName, factory));
        SQL_LOG(INFO, "add function module [%s]", moduleName.c_str());
        if (!factory->registerAggFunctions()) {
            SQL_LOG(ERROR, "register agg function failed, module[%s]", moduleName.c_str());
            return false;
        }
        if (!addFunctions(factory)) {
            SQL_LOG(ERROR, "add plugin agg functions failed, module[%s]",
                      moduleName.c_str());
            return false;
        }
    }
    return true;
}

bool AggFuncManager::addFunctions(AggFuncCreatorFactory *factory) {
    for (auto &iter : factory->getAggFuncCreators()) {
        const string &funcName = iter.first;
        if (_funcToCreator.find(funcName) != _funcToCreator.end()) {
            SQL_LOG(ERROR, "init agg function plugins failed: conflict function name[%s].",
                      funcName.c_str());
            return false;
        }
        _funcToCreator[funcName] = iter.second;
    }
    return true;
}

AggFunc *AggFuncManager::createAggFunction(const std::string &aggFuncName,
        const std::vector<ValueType> &inputTypes,
        const std::vector<std::string> &inputFields,
        const std::vector<std::string> &outputFields,
        bool isLocal)
{
    auto it = _funcToCreator.find(aggFuncName);
    if (it == _funcToCreator.end()) {
        return nullptr;
    }
    AggFunc *aggFunc = it->second->createFunction(
            inputTypes, inputFields, outputFields, isLocal);
    if (aggFunc != nullptr) {
        aggFunc->setName(aggFuncName);
    }
    return aggFunc;
}

bool AggFuncManager::registerFunctionInfos() {
    if (!_builtinFactory.registerFunctionInfos(_functionModels, _accSize)) {
        SQL_LOG(ERROR, "get agg function infos failed, module name [builtin]");
        return false;
    }
    for (auto it : _pluginsFactory) {
        if (!it.second->registerFunctionInfos(_functionModels, _accSize)) {
            SQL_LOG(ERROR, "get agg function infos failed, module name [%s]",
                      it.first.c_str());
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(sql);
