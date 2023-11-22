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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
struct ValueInlineOnDiskSKeyNode {
private:
    static constexpr uint8_t INVALID_NODE = 0;
    static constexpr uint8_t SKEY_DELETED_NODE = 1;
    static constexpr uint8_t PKEY_DELETED_NODE = 2;
    static constexpr uint8_t VALUE_NODE = 3;
    struct Meta {
        uint8_t status  : 2;
        bool isLastNode : 1;

        Meta() : status(INVALID_NODE), isLastNode(false) {}
    } __attribute__((packed));

    static_assert(sizeof(Meta) == 1, "Meta is not one byte");

public:
    typedef SKeyType Type;
    Meta meta;
    SKeyType skey;

    ValueInlineOnDiskSKeyNode() : skey(SKeyType()) {}

public:
    void SetPKeyDeleted(uint32_t _ts) { meta.status = PKEY_DELETED_NODE; }

    void SetSKeyDeleted(SKeyType _skey, uint32_t _ts)
    {
        skey = _skey;
        meta.status = SKEY_DELETED_NODE;
    }

    void SetSKeyNodeInfo(SKeyType _skey, uint32_t _ts)
    {
        skey = _skey;
        meta.status = VALUE_NODE;
    }

    void SetLastNode(bool isLastNode) { meta.isLastNode = isLastNode; }

    bool IsValidNode() const { return meta.status != INVALID_NODE; }

    bool IsLastNode() const { return meta.isLastNode; }

    bool IsPKeyDeleted() const { return meta.status == PKEY_DELETED_NODE; }

    bool IsSKeyDeleted() const { return meta.status == SKEY_DELETED_NODE; }

    bool HasValue() const { return meta.status == VALUE_NODE; }

} __attribute__((packed));
}} // namespace indexlib::index
