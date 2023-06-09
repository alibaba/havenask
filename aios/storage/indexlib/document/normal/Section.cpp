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
#include "indexlib/document/normal/Section.h"

#include <cassert>

using namespace std;

namespace indexlib { namespace document {

const uint32_t Section::MAX_TOKEN_PER_SECTION = 0xffff;
const uint32_t Section::DEFAULT_TOKEN_NUM = 8;
const uint32_t Section::MAX_SECTION_LENGTH = (1 << 11) - 1;

Token* Section::CreateToken(uint64_t hashKey, pos_t posIncrement, pospayload_t posPayload)
{
    if (_tokenUsed >= _tokenCapacity) {
        if (_tokenUsed == MAX_TOKEN_PER_SECTION) {
            return NULL;
        }

        uint32_t newCapacity = std::min((uint32_t)_tokenCapacity * 2, MAX_TOKEN_PER_SECTION);

        Token* newTokens = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, Token, newCapacity);

        memcpy((void*)newTokens, _tokens, _tokenUsed * sizeof(Token));
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _tokens, _tokenCapacity);
        _tokens = newTokens;
        _tokenCapacity = newCapacity;
    }

    Token* retToken = _tokens + _tokenUsed++;

    retToken->_posIncrement = posIncrement;
    retToken->_posPayload = posPayload;
    retToken->_termKey = hashKey;
    return retToken;
}

void Section::Reset()
{
    _tokenUsed = 0;
    _length = 0;
    _weight = 0;
}

bool Section::operator==(const Section& right) const
{
    if (this == &right) {
        return true;
    }
    if (_sectionId != right._sectionId || _length != right._length || _weight != right._weight) {
        return false;
    }

    if (_tokenUsed != right._tokenUsed)
        return false;
    for (int i = 0; i < _tokenUsed; ++i) {
        if (_tokens[i] != right._tokens[i])
            return false;
    }
    return true;
}
bool Section::operator!=(const Section& right) const { return !operator==(right); }

void Section::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_tokenUsed);
    for (size_t i = 0; i < _tokenUsed; i++) {
        dataBuffer.write(_tokens[i]);
    }
    dataBuffer.write(_sectionId);
    dataBuffer.write(_length);
    dataBuffer.write(_weight);
}

void Section::deserialize(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(_tokenUsed);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _tokens, _tokenCapacity);
    _tokens = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, Token, _tokenUsed);
    for (size_t i = 0; i < _tokenUsed; i++) {
        dataBuffer.read(_tokens[i]);
    }
    dataBuffer.read(_sectionId);
    dataBuffer.read(_length);
    dataBuffer.read(_weight);
    _tokenCapacity = _tokenUsed;
}
}} // namespace indexlib::document
