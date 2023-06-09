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
#include "build_service/analyzer/SimpleTokenizer.h"

#include "autil/StringUtil.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/analyzer/SimpleTokenizer.h"

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, SimpleTokenizer);

SimpleTokenizer::SimpleTokenizer(const std::string& delimiter)
    : _impl(std::make_unique<indexlibv2::analyzer::SimpleTokenizer>(delimiter))
{
}

SimpleTokenizer::~SimpleTokenizer() {}

void SimpleTokenizer::reset() { _impl->reset(); }

bool SimpleTokenizer::init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReaderPtr)
{
    KeyValueMap::const_iterator iter = parameters.find(indexlibv2::analyzer::DELIMITER);
    if (iter == parameters.end() || iter->second.empty()) {
        // use default delimiter
        _impl->SetDelimiter("\t");
    } else {
        _impl->SetDelimiter(iter->second);
    }
    return true;
}

void SimpleTokenizer::tokenize(const char* text, size_t textLen) { _impl->tokenize(text, textLen); }

bool SimpleTokenizer::next(Token& token) { return _impl->next(token); }

indexlibv2::analyzer::ITokenizer* SimpleTokenizer::clone() { return new SimpleTokenizer(_impl->GetDelimiter()); }

}} // namespace build_service::analyzer
