#ifndef ISEARCH_TOKENIZERCONFIG_H
#define ISEARCH_TOKENIZERCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/ModuleInfo.h>

BEGIN_HA3_NAMESPACE(config);

class TokenizerInfo : public autil::legacy::Jsonizable 
{
public:
    TokenizerInfo() {}
    ~TokenizerInfo() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
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
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("modules", _modules, _modules);
        json.Jsonize("tokenizers", _tokenizerInfos, _tokenizerInfos);
    }
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
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
    build_service::plugin::ModuleInfos _modules;
    TokenizerInfos _tokenizerInfos;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TokenizerConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_TOKENIZERCONFIG_H
