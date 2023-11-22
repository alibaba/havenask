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
#include "build_service/analyzer/PrefixTokenizerImpl.h"

#include <cstddef>
#include <string>

#include "autil/codec/UTF8Util.h"

using namespace std;

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, PrefixTokenizerImpl);

PrefixTokenizerImpl::PrefixTokenizerImpl(uint32_t maxTokenNum, bool includeSelf)
    : _maxTokenNum(maxTokenNum)
    , _includeSelf(includeSelf)
{
}

PrefixTokenizerImpl::~PrefixTokenizerImpl() {}

void PrefixTokenizerImpl::reset()
{
    _text = NULL;
    _textLen = 0;
    _position = 0;
    _posVec.clear();
}

bool PrefixTokenizerImpl::tokenize(const char* text, size_t len)
{
    _text = text;
    _textLen = len;
    _position = 0;
    _posVec.clear();

    size_t start = 0;
    while (start < len) {
        size_t charLen = 0;
        if (::autil::codec::UTF8Util::getNextCharUTF8(text, start, len, charLen)) {
            _posVec.push_back(start + charLen);
            start += charLen;
        } else {
            _posVec.clear(); // text is not a valid utf8, make tokenize fail
            return false;
        }
    }
    return true;
}

bool PrefixTokenizerImpl::next(Token& token)
{
    if (_position >= _maxTokenNum || _position >= _posVec.size()) {
        return false;
    }

    if (!_includeSelf && _textLen == _posVec[_position]) {
        return false;
    }

    token.getNormalizedText().assign(_text, _text + _posVec[_position]);
    token.setPosition(_position++);
    token.setIsRetrieve(true);
    token.setIsDelimiter(false);

    return true;
}

}} // namespace build_service::analyzer
