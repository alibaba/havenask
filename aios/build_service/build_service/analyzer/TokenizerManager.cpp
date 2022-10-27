#include "build_service/analyzer/TokenizerManager.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/analyzer/TokenizerModuleFactory.h"
#include "build_service/analyzer/BuildInTokenizerFactory.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/plugin/Module.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::plugin;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, TokenizerManager);

const std::string MODULE_FUNC_TOKENIZER = "_Tokenizer";

TokenizerManager::TokenizerManager(const ResourceReaderPtr &resourceReaderPtr)
    : _resourceReaderPtr(resourceReaderPtr)
{
}

TokenizerManager::~TokenizerManager() {
    map<string, Tokenizer*>::iterator iter = _tokenizerMap.begin();
    for (; iter != _tokenizerMap.end(); ++iter) {
        DELETE_AND_SET_NULL(iter->second);
    }
    _tokenizerMap.clear();
}

bool TokenizerManager::init(const TokenizerConfig &tokenizerConfig) {
    _tokenizerConfig = tokenizerConfig;
    const TokenizerInfos &tokenizerInfos = tokenizerConfig.getTokenizerInfos();
    for (size_t i = 0; i < tokenizerInfos.size(); i++) {
        const string &moduleName = tokenizerInfos[i].getModuleName();
        const string &tokenizerType = tokenizerInfos[i].getTokenizerType();
        const string &tokenizerName = tokenizerInfos[i].getTokenizerName();
        if (exist(tokenizerName)) {
            string errorMsg = "duplicated TokenizerName : " + tokenizerName;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        const KeyValueMap &parameter = tokenizerInfos[i].getParameters();
        TokenizerModuleFactory *factory = getModuleFactory(moduleName);
        if (!factory) {
            string errorMsg = "get TokenizerModuleFactory failed : " + moduleName;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        Tokenizer* tokenizer = factory->createTokenizer(tokenizerType);
        if (!tokenizer) {
            string errorMsg = "create tokenizer failed, tokenizer type : " + tokenizerType;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (!tokenizer->init(parameter, _resourceReaderPtr)) {
            string errorMsg = "init tokenizer failed, tokenizer type : " + tokenizerType;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            DELETE_AND_SET_NULL(tokenizer);
            return false;
        }
        addTokenizer(tokenizerName, tokenizer);
    }
    return true;
}

void TokenizerManager::addTokenizer(const string &tokenizerName, Tokenizer *tokenizer) {
    _tokenizerMap[tokenizerName] = tokenizer;
}

bool TokenizerManager::exist(const string &tokenizerName) const {
    return _tokenizerMap.find(tokenizerName) != _tokenizerMap.end();
}

TokenizerModuleFactory* TokenizerManager::getModuleFactory(
        const string &moduleName)
{
    if (PlugInManager::isBuildInModule(moduleName)) {
        if (!_buildInFactoryPtr) {
            _buildInFactoryPtr.reset(new BuildInTokenizerFactory());
            if (!_buildInFactoryPtr->init(KeyValueMap(), _resourceReaderPtr)) {
                string errorMsg = "init BuildInTokenizerFactory failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                _buildInFactoryPtr.reset();
                return NULL;
            }
        }
        return _buildInFactoryPtr.get();
    } else {
        const PlugInManagerPtr& pluginManagerPtr = getPluginManager();
        if (!pluginManagerPtr) {
            string errorMsg = "get plugin manager failed!.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return NULL;
        }
        Module *module = _plugInManagerPtr->getModule(moduleName);
        if (module == NULL) {
            string errorMsg = "GetModule [" + moduleName + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return NULL;
        }
        TokenizerModuleFactory *factory = dynamic_cast<TokenizerModuleFactory *>(
                module->getModuleFactory());
        if (!factory) {
            string errorMsg = "Get TokenizerModuleFactory failed, moduleName: " + moduleName;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return NULL;
        }
        return factory;
    }
}

Tokenizer* TokenizerManager::getTokenizerByName(const string &name) const {
    std::map<std::string, Tokenizer*>::const_iterator iter =
        _tokenizerMap.find(name);
    if (iter != _tokenizerMap.end()) {
        return iter->second->allocate();
    }
    return NULL;
}

const plugin::PlugInManagerPtr& TokenizerManager::getPluginManager() {
    if (_plugInManagerPtr) {
        return _plugInManagerPtr;
    }
    assert(_resourceReaderPtr);
    const ModuleInfos& moduleInfos = _tokenizerConfig.getModuleInfos();
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(
                    _resourceReaderPtr, _resourceReaderPtr->getPluginPath(),
                    MODULE_FUNC_TOKENIZER));
    if (!plugInManagerPtr->addModules(moduleInfos)) {
        string errorMsg = "Failed to create PlugInManager";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return _plugInManagerPtr;// empty
    }
    _plugInManagerPtr = plugInManagerPtr;
    return _plugInManagerPtr;
}

}
}
