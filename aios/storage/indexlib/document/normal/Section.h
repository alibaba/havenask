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
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/DynamicBuf.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/normal/Token.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib { namespace document {

#pragma pack(push, 2)

class Section
{
public:
    static const uint32_t MAX_TOKEN_PER_SECTION;
    static const uint32_t DEFAULT_TOKEN_NUM;
    static const uint32_t MAX_SECTION_LENGTH;

public:
    Section(uint32_t tokenCount = DEFAULT_TOKEN_NUM, autil::mem_pool::PoolBase* pool = NULL)
        : _pool(pool)
        , _sectionId(INVALID_SECID)
        , _length(0)
        , _weight(0)
        , _tokenUsed(0)
        , _tokenCapacity(std::min(tokenCount, MAX_TOKEN_PER_SECTION))
    {
        if (_tokenCapacity != 0) {
            _tokens = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, Token, _tokenCapacity);
        } else {
            _tokens = NULL;
        }
    }

    ~Section()
    {
        if (_tokens) {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _tokens, _tokenCapacity);
            _tokens = NULL;
        }
    }

public:
    // used in build process for reusing token object
    Token* CreateToken(uint64_t hashKey, pos_t posIncrement = 0, pospayload_t posPayload = 0);

    void Reset();

    size_t GetTokenCount() const { return _tokenUsed; }

    Token* GetToken(int32_t idx)
    {
        assert(idx < _tokenUsed && idx >= 0);
        return _tokens + idx;
    }

    const Token* GetToken(int32_t idx) const
    {
        assert(idx < _tokenUsed && idx >= 0);
        return _tokens + idx;
    }

    void SetSectionId(sectionid_t sectionId) { _sectionId = sectionId; }
    sectionid_t GetSectionId() const { return _sectionId; }

    section_weight_t GetWeight() const { return _weight; }
    void SetWeight(section_weight_t weight) { _weight = weight; }

    section_len_t GetLength() const { return _length; }
    void SetLength(section_len_t length) { _length = length; }

    bool operator==(const Section& right) const;
    bool operator!=(const Section& right) const;

    void SetTokenUsed(int32_t tokenUsed) { _tokenUsed = tokenUsed; }

    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer);

private:
    // for test
    section_len_t GetCapacity() const { return _tokenCapacity; }

private:
    autil::mem_pool::PoolBase* _pool;
    Token* _tokens;

    sectionid_t _sectionId;
    section_len_t _length;
    section_weight_t _weight;
    section_len_t _tokenUsed;
    section_len_t _tokenCapacity;
    friend class SectionTest;
};

#pragma pack(pop)
}} // namespace indexlib::document
