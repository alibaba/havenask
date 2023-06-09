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
#include "indexlib/document/normal/tokenize/AnalyzerToken.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace document {

AnalyzerToken::AnalyzerToken() : _text(), _normalizedText(), _pos((uint32_t)-1), _posPayLoad(0) {}

AnalyzerToken::AnalyzerToken(const string& text, uint32_t pos)
    : _text(text)
    , _normalizedText(text)
    , _pos(pos)
    , _posPayLoad(0)
{
}

AnalyzerToken::AnalyzerToken(const string& text, uint32_t pos, const string& normalizedText, bool isStopWord)
    : _text(text)
    , _normalizedText(normalizedText)
    , _pos(pos)
    , _posPayLoad(0)
{
    _tokenProperty.setStopWord(isStopWord);
}

AnalyzerToken::~AnalyzerToken() {}

void AnalyzerToken::serialize(DataBuffer& dataBuffer) const
{
    dataBuffer.write(_text);
    dataBuffer.write(_normalizedText);
    dataBuffer.write(_pos);
    dataBuffer.write(_tokenProperty);
    dataBuffer.write(_posPayLoad);
}

void AnalyzerToken::deserialize(DataBuffer& dataBuffer)
{
    dataBuffer.read(_text);
    dataBuffer.read(_normalizedText);
    dataBuffer.read(_pos);
    dataBuffer.read(_tokenProperty);
    dataBuffer.read(_posPayLoad);
}
}} // namespace indexlib::document
