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
#include "build_service/analyzer/SuffixPrefixTokenizerImpl.h"

#include "autil/codec/UTF8Util.h"

using namespace std;

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, SuffixPrefixTokenizerImpl);

SuffixPrefixTokenizerImpl::SuffixPrefixTokenizerImpl(uint32_t maxSuffixLen)
    : _maxSuffixLen(maxSuffixLen)
    , _text(NULL)
    , _textLen(0)
    , _pos_index(-1)
{
}

SuffixPrefixTokenizerImpl::~SuffixPrefixTokenizerImpl() {}

void SuffixPrefixTokenizerImpl::reset()
{
    _text = NULL;
    _textLen = 0;
    _pos_index = -1;
    _posVec.clear();
}

bool SuffixPrefixTokenizerImpl::tokenize(const char* text, size_t len)
{
    _text = text;
    _textLen = len;
    _pos_index = -1;
    _posVec.clear();

    size_t start = 0;
    while (start < len) {
        size_t charLen = 0;
        if (::autil::codec::UTF8Util::getNextCharUTF8(text, start, len, charLen)) {
            _posVec.insert(_posVec.begin(), start);
            _pos_index = 0;
            start += charLen;
        } else {
            _posVec.clear(); // text is not a valid utf8, make tokenize fail
            return false;
        }
    }
    return true;
}

bool SuffixPrefixTokenizerImpl::next(Token& token)
{
    if (_pos_index < 0 || _pos_index >= (int32_t)_posVec.size() || _pos_index >= (int32_t)_maxSuffixLen) {
        return false;
    }

    token.getNormalizedText().assign(_text + _posVec[_pos_index], _textLen - _posVec[_pos_index]);
    BS_LOG(INFO, "%s", token.getNormalizedText().c_str());
    token.setPosition(_posVec[_pos_index]);
    ++_pos_index;
    token.setIsRetrieve(true);
    token.setIsDelimiter(false);

    return true;
}

}} // namespace build_service::analyzer
