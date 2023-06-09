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
#include "indexlib/document/normal/Token.h"

#include <cassert>

using namespace std;
namespace indexlib { namespace document {

const string Token::DUMMY_TERM_STRING = "#$%^&*()_+!@~|\\";

bool Token::operator==(const Token& right) const
{
    if (this == &right) {
        return true;
    }

    return _termKey == right._termKey && _posIncrement == right._posIncrement && _posPayload == right._posPayload;
}

bool Token::operator!=(const Token& right) const { return !operator==(right); }

void Token::CreatePairToken(const Token& firstToken, const Token& nextToken, Token& pairToken)
{
    assert(0);
    // TODO
}

void Token::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_termKey);
    dataBuffer.write(_posIncrement);
    dataBuffer.write(_posPayload);
}

void Token::deserialize(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(_termKey);
    dataBuffer.read(_posIncrement);
    dataBuffer.read(_posPayload);
}
}} // namespace indexlib::document
