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
#pragma once
#include <memory>

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/HashAlgorithm.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib { namespace document {

#pragma pack(push, 1)

class Token
{
public:
    static const int TOKEN_SERIALIZE_MAGIC;
    static const std::string DUMMY_TERM_STRING;

private:
    Token() : _termKey(0), _posIncrement(0), _posPayload(0) {}

public:
    uint64_t GetHashKey() const { return _termKey; }

    void SetPosIncrement(pos_t posInc) { _posIncrement = posInc; }

    pos_t GetPosIncrement() const { return _posIncrement; }

    void SetPosPayload(pospayload_t payload) { _posPayload = payload; }

    pospayload_t GetPosPayload() const { return _posPayload; }

    bool operator==(const Token& right) const;
    bool operator!=(const Token& right) const;

    static void CreatePairToken(const Token& firstToken, const Token& nextToken, Token& pairToken);

    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer);

private:
    uint64_t _termKey;        // 8 byte
    pos_t _posIncrement;      // 4 byte
    pospayload_t _posPayload; // 1 byte

private:
    friend class TokenTest;
    friend class Section;
};

#pragma pack(pop)

}} // namespace indexlib::document
