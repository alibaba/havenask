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

#include "CustomTokenizerModuleFactory.h"
#include "JiebaTokenizer.h"


using pluginplatform::analyzer_plugins::JiebaTokenizer;
using namespace build_service::analyzer;

namespace pluginplatform {
namespace analyzer_plugins {

BS_LOG_SETUP(plug, CustomTokenizerModuleFactory);

Tokenizer* CustomTokenizerModuleFactory::createTokenizer(const std::string& tokenizerType)
{
    Tokenizer* tokenizer = nullptr;
    if ( tokenizerType == "jieba" ){
        tokenizer = new JiebaTokenizer();
    }
    BS_LOG(ERROR,"Tokenizer type %s Not Found.",tokenizerType.c_str());
    return tokenizer;
}
}}

extern "C" build_service::plugin::ModuleFactory* createFactory_Tokenizer()
{
    return (build_service::plugin::ModuleFactory*) new (std::nothrow) pluginplatform::analyzer_plugins::CustomTokenizerModuleFactory();
}

extern "C" void destroyFactory_Tokenizer(build_service::plugin::ModuleFactory *factory)
{
    factory->destroy();
}
