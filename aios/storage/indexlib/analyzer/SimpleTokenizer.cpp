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
#include "indexlib/analyzer/SimpleTokenizer.h"

#include "autil/StringUtil.h"

namespace indexlibv2 { namespace analyzer {
AUTIL_LOG_SETUP(indexlib.analyzer, SimpleTokenizer);

SimpleTokenizer::SimpleTokenizer(const std::string& delimiter)
{
    _delimiter = delimiter;
    reset();
}

void SimpleTokenizer::reset()
{
    _curPos = 0;
    _position = 0;
    _text = nullptr;
    _textLen = 0;
    _begin = nullptr;
    _end = nullptr;
}

void SimpleTokenizer::tokenize(const char* text, size_t textLen)
{
    _text = text;
    _textLen = textLen;

    autil::StringUtil::sundaySearch(_text, _textLen, _delimiter.c_str(), _delimiter.size(), _posVec);
    _curPos = 0;
    _position = 0;
    _begin = _text;
    if (!_posVec.empty()) {
        _end = _text + _posVec[0];
    } else {
        _end = _text + _textLen;
    }
}

bool SimpleTokenizer::next(Token& token)
{
    if (_begin >= _text + _textLen) {
        return false;
    }

    if (_begin != _end) {
        token.getNormalizedText().assign(_begin, _end);
        token.setPosition(_position++);
        _begin = _end;
        token.setIsDelimiter(false);
        token.setIsRetrieve(true);
    } else {
        token.getNormalizedText().assign(_delimiter);
        token.setPosition(_position++);
        token.setIsDelimiter(true);
        token.setIsRetrieve(false);
        _begin += _delimiter.size();
        ++_curPos;
        if (_curPos >= _posVec.size()) {
            _end = _text + _textLen;
        } else {
            _end = _text + _posVec[_curPos];
        }
    }
    return true;
}

ITokenizer* SimpleTokenizer::clone() { return new SimpleTokenizer(_delimiter); }

}} // namespace indexlibv2::analyzer
