/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/util/Log.h"

namespace build_service { namespace analyzer {

class TokenizerInfo : public autil::legacy::Jsonizable
{
public:
    TokenizerInfo() {}
    ~TokenizerInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("tokenizer_type", _tokenizerType, _tokenizerType);
        json.Jsonize("tokenizer_name", _tokenizerName, _tokenizerName);
        json.Jsonize("module_name", _moduleName, _moduleName);
        json.Jsonize("parameters", _params, _params);
    }
    const std::string& getTokenizerType() const { return _tokenizerType; }
    void setTokenizerType(const std::string& type) { _tokenizerType = type; }
    const std::string& getTokenizerName() const { return _tokenizerName; }
    void setTokenizerName(const std::string& name) { _tokenizerName = name; }
    const std::string& getModuleName() const { return _moduleName; }
    void setModuleName(const std::string& moduleName) { _moduleName = moduleName; }
    const KeyValueMap& getParameters() const { return _params; }
    void setParameters(const KeyValueMap& params) { _params = params; }

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
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("modules", _modules, _modules);
        json.Jsonize("tokenizers", _tokenizerInfos, _tokenizerInfos);
    }

public:
    const plugin::ModuleInfos& getModuleInfos() const { return _modules; }
    const TokenizerInfos& getTokenizerInfos() const { return _tokenizerInfos; }
    void addTokenizerInfo(const TokenizerInfo& info) { _tokenizerInfos.push_back(info); }
    bool tokenizerNameExist(const std::string& tokenizerName) const
    {
        for (size_t i = 0; i < _tokenizerInfos.size(); i++) {
            if (tokenizerName == _tokenizerInfos[i].getTokenizerName()) {
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

}} // namespace build_service::analyzer
