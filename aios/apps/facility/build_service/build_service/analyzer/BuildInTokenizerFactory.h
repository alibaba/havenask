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

#include <string>

#include "build_service/analyzer/TokenizerModuleFactory.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"

namespace ws {
class AliTokenizerFactory;
}

namespace build_service { namespace analyzer {

class BuildInTokenizerFactory : public TokenizerModuleFactory
{
public:
    BuildInTokenizerFactory();
    virtual ~BuildInTokenizerFactory();

private:
    BuildInTokenizerFactory(const BuildInTokenizerFactory&);
    BuildInTokenizerFactory& operator=(const BuildInTokenizerFactory&);

public:
    virtual bool init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReader);
    virtual void destroy();
    virtual Tokenizer* createTokenizer(const std::string& tokenizerType);

private:
    bool initAliTokenizerFactory();
    Tokenizer* createAliWsTokenizer(const std::string& tokenizerType);

private:
    std::string _aliTokenizerConfigPath;
    ws::AliTokenizerFactory* _aliTokenizerFactory;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildInTokenizerFactory);

}} // namespace build_service::analyzer
