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
#include "build_service/analyzer/PrefixTokenizer.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, PrefixTokenizer);

const uint32_t PrefixTokenizer::DEFAULT_MAX_TOKEN_NUM = 8;

static const string MAX_TOKEN_NUM = "max_token_num";
static const string INCLUDE_SELF = "include_self";
static const string SEPERATOR = "seperator";

PrefixTokenizer::PrefixTokenizer() : _maxTokenNum(DEFAULT_MAX_TOKEN_NUM), _includeSelf(false)
{
    _seperator = string(1, MULTI_VALUE_SEPARATOR);
    _text = NULL;
    _position = 0;
    _cursor = 0;
}

PrefixTokenizer::PrefixTokenizer(uint32_t maxTokenNum, bool includeSelf, const string& seperator)
    : _maxTokenNum(maxTokenNum)
    , _includeSelf(includeSelf)
    , _seperator(seperator)
{
    _text = NULL;
    _position = 0;
    _cursor = 0;
}

PrefixTokenizer::~PrefixTokenizer() { clear(); }

bool PrefixTokenizer::init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReader)
{
    KeyValueMap::const_iterator it = parameters.find(MAX_TOKEN_NUM);
    if (it != parameters.end()) {
        uint32_t maxTokenNum;
        if (!StringUtil::fromString<uint32_t>(it->second, maxTokenNum)) {
            BS_LOG(ERROR, "max_token_num[%s] invalid", it->second.c_str());
            return false;
        }
        _maxTokenNum = maxTokenNum;
    }

    it = parameters.find(INCLUDE_SELF);
    if (it != parameters.end()) {
        bool includeSelf;
        if (!StringUtil::fromString<bool>(it->second, includeSelf)) {
            BS_LOG(ERROR, "include_self [%s] invalid", it->second.c_str());
            return false;
        }
        _includeSelf = includeSelf;
    }

    it = parameters.find(SEPERATOR);
    if (it != parameters.end()) {
        _seperator = it->second;
    }

    return true;
}

void PrefixTokenizer::clear()
{
    for (size_t i = 0; i < _impls.size(); i++) {
        delete _impls[i];
    }
    _impls.clear();
}

void PrefixTokenizer::tokenize(const char* text, size_t len)
{
    _text = text;
    _cursor = 0;
    _position = 0;
    clear();
    StringView input(text, len);
    vector<StringView> inputVec = StringTokenizer::constTokenize(input, _seperator, StringTokenizer::TOKEN_LEAVE_AS);
    for (size_t i = 0; i < inputVec.size(); i++) {
        PrefixTokenizerImpl* impl = new PrefixTokenizerImpl(_maxTokenNum, _includeSelf);
        if (!impl->tokenize(inputVec[i].data(), inputVec[i].length())) {
            clear();
            delete impl;
            break;
        }
        _impls.push_back(impl);
    }
}

bool PrefixTokenizer::next(Token& token)
{
    while (_cursor < _impls.size()) {
        PrefixTokenizerImpl* impl = _impls[_cursor];
        if (impl->next(token)) {
            token.setPosition(_position++);
            return true;
        }
        _cursor++;
    }
    return false;
}

indexlibv2::analyzer::ITokenizer* PrefixTokenizer::clone()
{
    return new PrefixTokenizer(_maxTokenNum, _includeSelf, _seperator);
}

}} // namespace build_service::analyzer
