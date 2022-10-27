#ifndef ISEARCH_BS_TOKENIZERCONFIG_H
#define ISEARCH_BS_TOKENIZERCONFIG_H

#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>
#include "build_service/plugin/ModuleInfo.h"

namespace build_service {
namespace analyzer {

class TokenizerInfo : public autil::legacy::Jsonizable
{
public:
    TokenizerInfo() {}
    ~TokenizerInfo() {}
public:
    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("tokenizer_type", _tokenizerType, _tokenizerType);
        json.Jsonize("tokenizer_name", _tokenizerName, _tokenizerName);
        json.Jsonize("module_name", _moduleName, _moduleName);
        json.Jsonize("parameters", _params, _params);
    }
    const std::string& getTokenizerType() const {
        return _tokenizerType;
    }
    void setTokenizerType(const std::string& type) {
        _tokenizerType = type;
    }
    const std::string& getTokenizerName() const {
        return _tokenizerName;
    }
    void setTokenizerName(const std::string& name) {
        _tokenizerName = name;
    }
    const std::string& getModuleName() const {
        return _moduleName;
    }
    void setModuleName(const std::string& moduleName) {
        _moduleName = moduleName;
    }
    const KeyValueMap& getParameters() const {
        return _params;
    }
    void setParameters(const KeyValueMap &params) {
        _params = params;
    }
private:
    std::string _tokenizerType;
    std::string _tokenizerName;
    std::string _moduleName;
    KeyValueMap _params;
};

typedef std::vector<TokenizerInfo> TokenizerInfos;
class TokenizerConfig : public autil::legacy::Jsonizable
{
public:
    TokenizerConfig() {}
    ~TokenizerConfig() {}
public:
    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("modules", _modules, _modules);
        json.Jsonize("tokenizers", _tokenizerInfos, _tokenizerInfos);
    }
public:
    const plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const TokenizerInfos& getTokenizerInfos() const {
        return _tokenizerInfos;
    }
    void addTokenizerInfo(const TokenizerInfo& info) {
        _tokenizerInfos.push_back(info);
    }
    bool tokenizerNameExist(const std::string& tokenizerName) const {
        for (size_t i = 0; i < _tokenizerInfos.size(); i++) {
            if (tokenizerName
                == _tokenizerInfos[i].getTokenizerName())
            {
                return true;
            }
        }
        return false;
    }
private:
    plugin::ModuleInfos _modules;
    TokenizerInfos _tokenizerInfos;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TokenizerConfig);

}
}

#endif //ISEARCH_BS_TOKENIZERCONFIG_H
