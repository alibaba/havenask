#include <ha3/sql/ops/tvf/TvfFuncManager.h>
#include <build_service/plugin/Module.h>

using namespace std;
using namespace build_service::plugin;
using namespace build_service::config;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, TvfFuncManager);

TvfFuncManager::TvfFuncManager()
    : _pluginManager(nullptr)
{
}

TvfFuncManager::~TvfFuncManager() {
    DELETE_AND_SET_NULL(_pluginManager);
}

bool TvfFuncManager::init(const std::string &configPath,
                          const HA3_NS(config)::SqlTvfPluginConfig &tvfPluginConfig)
{
    _resourceReaderPtr.reset(new ResourceReader(configPath));
    if (!initBuiltinFactory()) {
        return false;
    }
    if (!initPluginFactory(configPath, tvfPluginConfig)) {
        return false;
    }
    if (!addDefaultTvfInfo()) {
        SQL_LOG(ERROR, "add defalut tvf info failed.");
        return false;
    }
    if (!initTvfProfile(tvfPluginConfig.tvfProfileInfos)) {
        return false;
    }
    return true;
}

bool TvfFuncManager::initBuiltinFactory() {
    if (!_builtinFactory.registerTvfFunctions()) {
        SQL_LOG(ERROR, "register builtinTvfFuncCreatorFactory failed.");
        return false;
    }
    if (!addFunctions(&_builtinFactory)) {
        SQL_LOG(ERROR, "add builtin tvf functions failed.");
        return false;
    }
    return true;
}

bool TvfFuncManager::addDefaultTvfInfo() {
    for (auto iter : _funcToCreator) {
        auto creator = iter.second;
        const auto &info = creator->getDefaultTvfInfo();
        if (info.empty()) {
            continue;
        }
        const std::string &tvfName = info.tvfName;
        if (_tvfNameToCreator.find(tvfName) != _tvfNameToCreator.end()) {
            SQL_LOG(ERROR, "add tvf function failed: conflict tvf name[%s].",
                    tvfName.c_str());
            return false;
        }
        _tvfNameToCreator[tvfName] = creator;
    }
    return true;
}

bool TvfFuncManager::initPluginFactory(const std::string &configPath,
                                       const SqlTvfPluginConfig &tvfPluginConfig)
{
    _pluginManager = new PlugInManager(_resourceReaderPtr, MODULE_SQL_TVF_FUNCTION);
    if (!_pluginManager->addModules(tvfPluginConfig.modules)) {
        SQL_LOG(ERROR, "load sql tvf function modules failed : %s.",
                  ToJsonString(tvfPluginConfig.modules).c_str());
        return false;
    }
    auto &modules = tvfPluginConfig.modules;
    for (size_t i = 0; i < modules.size(); ++i) {
        const string &moduleName = modules[i].moduleName;
        Module *module = _pluginManager->getModule(moduleName);
        if (module == nullptr) {
            SQL_LOG(ERROR, "Get module [%s] failed", moduleName.c_str());
            return false;
        }
        TvfFuncCreatorFactory *factory = dynamic_cast<TvfFuncCreatorFactory *>(
                module->getModuleFactory());
        if (factory == nullptr) {
            SQL_LOG(ERROR, "invalid TvfFuncCreatorFactory in module[%s].",
                      moduleName.c_str());
            return false;
        }
        if (!factory->init(modules[i].parameters, _resourceReaderPtr, &_resourceContainer)) {
            SQL_LOG(ERROR, "init TvfFuncCreatorFactory in module[%s] failed.",
                      moduleName.c_str());
            return false;
        }
        _pluginsFactory.push_back(make_pair(moduleName, factory));
        SQL_LOG(INFO, "add function module [%s]", moduleName.c_str());
        if (!factory->registerTvfFunctions()) {
            SQL_LOG(ERROR, "register tvf function failed, module[%s]", moduleName.c_str());
            return false;
        }
        if (!addFunctions(factory)) {
            SQL_LOG(ERROR, "add plugin tvf functions failed, module[%s]",
                      moduleName.c_str());
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::regTvfModels(iquan::TvfModels &tvfModels) {
    for (auto iter : _funcToCreator) {
        if (!iter.second->regTvfModels(tvfModels)) {
            SQL_LOG(ERROR, "register tvf models [%s] failed", iter.first.c_str());
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::initTvfFuncCreator() {
    for (auto iter : _funcToCreator) {
        if (!iter.second->init(_resourceReaderPtr.get())) {
            SQL_LOG(ERROR, "init tvf function creator [%s] failed", iter.first.c_str());
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::initTvfProfile(const SqlTvfProfileInfos &sqlTvfProfileInfos) {
    for (const auto &sqlTvfProfileInfo : sqlTvfProfileInfos) {
        auto iter = _funcToCreator.find(sqlTvfProfileInfo.funcName);
        if (iter == _funcToCreator.end()) {
            SQL_LOG(ERROR, "can not find tvf [%s]", sqlTvfProfileInfo.funcName.c_str());
            return false;
        }
        auto creator = iter->second;
        creator->addTvfFunction(sqlTvfProfileInfo);
        if (_tvfNameToCreator.find(sqlTvfProfileInfo.tvfName) != _tvfNameToCreator.end()) {
            SQL_LOG(ERROR, "add tvf function failed: conflict tvf name[%s].",
                      sqlTvfProfileInfo.tvfName.c_str());
            return false;
        }
        _tvfNameToCreator[sqlTvfProfileInfo.tvfName] = creator;
    }
    if (!initTvfFuncCreator()) {
        return false;
    }
    return true;
}

bool TvfFuncManager::addFunctions(TvfFuncCreatorFactory *factory) {
    for (auto &iter : factory->getTvfFuncCreators()) {
        const string &funcName = iter.first;
        if (_funcToCreator.find(funcName) != _funcToCreator.end()) {
            SQL_LOG(ERROR, "init tvf function plugins failed: conflict function name[%s].",
                      funcName.c_str());
            return false;
        }
        _funcToCreator[funcName] = iter.second;
    }
    return true;
}

TvfFunc *TvfFuncManager::createTvfFunction(const std::string &tvfName) {
    auto it = _tvfNameToCreator.find(tvfName);
    if (it == _tvfNameToCreator.end()) {
        SQL_LOG(WARN, "tvf [%s]'s creator not found.", tvfName.c_str());
        return nullptr;
    }
    TvfFunc *tvfFunc = it->second->createFunction(tvfName);
    return tvfFunc;
}

END_HA3_NAMESPACE(sql);
