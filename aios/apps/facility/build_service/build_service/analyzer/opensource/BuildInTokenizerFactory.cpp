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
#include "build_service/analyzer/BuildInTokenizerFactory.h"

#include "build_service/analyzer/PrefixTokenizer.h"
#include "build_service/analyzer/SimpleTokenizer.h"
#include "build_service/analyzer/SingleWSTokenizer.h"
#include "build_service/analyzer/SuffixPrefixTokenizer.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/analyzer/AnalyzerDefine.h"

using namespace std;
using namespace indexlibv2::analyzer;

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, BuildInTokenizerFactory);

BuildInTokenizerFactory::BuildInTokenizerFactory() {}

BuildInTokenizerFactory::~BuildInTokenizerFactory() {}

void BuildInTokenizerFactory::destroy() {}

bool BuildInTokenizerFactory::init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReader)
{
    return true;
}

Tokenizer* BuildInTokenizerFactory::createTokenizer(const string& tokenizerType)
{
    Tokenizer* tokenizer = NULL;
    if (tokenizerType == SIMPLE_ANALYZER) {
        tokenizer = new SimpleTokenizer();
    } else if (tokenizerType == SINGLEWS_ANALYZER) {
        tokenizer = new SingleWSTokenizer();
    } else if (tokenizerType == PREFIX_ANALYZER) {
        tokenizer = new PrefixTokenizer();
    } else if (tokenizerType == SUFFIX_PREFIX_ANALYZER) {
        tokenizer = new SuffixPrefixTokenizer();
    } else {
        tokenizer = new SingleWSTokenizer();
    }
    return tokenizer;
}

}} // namespace build_service::analyzer
