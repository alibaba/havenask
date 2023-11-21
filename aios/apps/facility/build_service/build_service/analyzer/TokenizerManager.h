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

#include <map>
#include <memory>
#include <string>

#include "build_service/analyzer/TokenizerConfig.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace indexlibv2::analyzer {
class ITokenizer;
}

namespace build_service { namespace plugin {
class PlugInManager;
}} // namespace build_service::plugin

namespace build_service { namespace config {
class ResourceReader;
}} // namespace build_service::config

namespace build_service { namespace analyzer {

class Tokenizer;
class TokenizerModuleFactory;
class BuildInTokenizerFactory;

class TokenizerManager
{
public:
    TokenizerManager(const std::shared_ptr<config::ResourceReader>& resourceReaderPtr);
    ~TokenizerManager();

private:
    TokenizerManager(const TokenizerManager&);
    TokenizerManager& operator=(const TokenizerManager&);

public:
    bool init(const TokenizerConfig& tokenizerConfig);
    indexlibv2::analyzer::ITokenizer* getTokenizerByName(const std::string& name) const;

public:
    // for test
    void setBuildInFactory(const std::shared_ptr<BuildInTokenizerFactory>& buildInFactoryPtr)
    {
        _buildInFactoryPtr = buildInFactoryPtr;
    }
    void addTokenizer(const std::string& tokenizerName, Tokenizer* tokenizer);

private:
    bool exist(const std::string& tokenizerName) const;
    TokenizerModuleFactory* getModuleFactory(const std::string& moduleName);
    const std::shared_ptr<plugin::PlugInManager>& getPluginManager();

private:
    std::map<std::string, Tokenizer*> _tokenizerMap;
    std::shared_ptr<config::ResourceReader> _resourceReaderPtr;
    std::shared_ptr<plugin::PlugInManager> _plugInManagerPtr;
    std::shared_ptr<BuildInTokenizerFactory> _buildInFactoryPtr;
    TokenizerConfig _tokenizerConfig;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TokenizerManager);

}} // namespace build_service::analyzer
