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
#include "build_service/analyzer/SuffixPrefixTokenizer.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, SuffixPrefixTokenizer);

const uint32_t SuffixPrefixTokenizer::DEFAULT_SUFFIX_LEN = 8;

static const string MAX_SUFFIX_LEN = "max_suffix_len"; // 最多分多少个后缀词
static const string SEPERATOR = "seperator";           // 多值分隔符
static const string MAX_PREFIX_LEN = "max_prefix_len"; // 前缀分词最大长度

SuffixPrefixTokenizer::SuffixPrefixTokenizer() : _maxSuffixLen(DEFAULT_SUFFIX_LEN), _maxPrefixLen(DEFAULT_SUFFIX_LEN)
{
    _seperator = string(1, MULTI_VALUE_SEPARATOR);
    _text = NULL;
    _position = 0;
    _cursor = 0;
}

SuffixPrefixTokenizer::SuffixPrefixTokenizer(uint32_t maxSuffixLen, uint32_t maxPrefixLen, const string& seperator)

    : _maxSuffixLen(maxSuffixLen)
    , _seperator(seperator)
    , _maxPrefixLen(maxPrefixLen)
{
    _text = NULL;
    _position = 0;
    _cursor = 0;
}

SuffixPrefixTokenizer::~SuffixPrefixTokenizer() { clear(); }

bool SuffixPrefixTokenizer::init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReader)
{
    KeyValueMap::const_iterator it = parameters.find(MAX_SUFFIX_LEN);
    if (it != parameters.end()) {
        uint32_t maxSuffixLen = 0;
        if (!StringUtil::fromString<uint32_t>(it->second, maxSuffixLen)) {
            BS_LOG(ERROR, "max_suffix_len [%s] invalid", it->second.c_str());
            return false;
        }
        _maxSuffixLen = maxSuffixLen;
    }

    it = parameters.find(MAX_PREFIX_LEN);
    if (it != parameters.end()) {
        uint32_t maxPrefixLen = 0;
        if (!StringUtil::fromString<uint32_t>(it->second, maxPrefixLen)) {
            BS_LOG(ERROR, "max_prefix_len [%s] invalid", it->second.c_str());
            return false;
        }
        _maxPrefixLen = maxPrefixLen;
    }

    it = parameters.find(SEPERATOR);
    if (it != parameters.end()) {
        _seperator = it->second;
    }

    return true;
}

void SuffixPrefixTokenizer::clear()
{
    for (size_t i = 0; i < _suffixStrBuf.size(); i++) {
        delete _suffixStrBuf[i];
    }
    _suffixStrBuf.clear();
    for (size_t i = 0; i < _impls.size(); i++) {
        delete _impls[i];
    }
    _impls.clear();
}

void SuffixPrefixTokenizer::tokenize(const char* text, size_t len)
{
    _text = text;
    _cursor = 0;
    _position = 0;
    clear();
    StringView input(text, len);
    vector<StringView> inputVec = StringTokenizer::constTokenize(input, _seperator, StringTokenizer::TOKEN_LEAVE_AS);
    for (size_t i = 0; i < inputVec.size(); i++) {
        SuffixPrefixTokenizerImpl* impl = new SuffixPrefixTokenizerImpl(_maxSuffixLen);
        if (!impl->tokenize(inputVec[i].data(), inputVec[i].length())) {
            clear();
            delete impl;
            break;
        }
        Token token;
        while (impl->next(token)) {
            string* tmpStr = new string(token.getNormalizedText().c_str(), token.getNormalizedText().length());
            _suffixStrBuf.push_back(tmpStr);
            PrefixTokenizerImpl* prefix = new PrefixTokenizerImpl(_maxPrefixLen, true);
            prefix->tokenize(tmpStr->data(), tmpStr->size());
            _impls.push_back(prefix);
        }
        DELETE_AND_SET_NULL(impl);
    }
}

bool SuffixPrefixTokenizer::next(Token& token)
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

Tokenizer* SuffixPrefixTokenizer::clone()
{
    return new SuffixPrefixTokenizer(_maxSuffixLen, _maxPrefixLen, _seperator);
}

}} // namespace build_service::analyzer
