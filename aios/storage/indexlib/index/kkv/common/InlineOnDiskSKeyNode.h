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

#include <cstdint>
#include <type_traits>

namespace indexlibv2::index {

template <typename SKeyType>
struct InlineOnDiskSKeyNode {
private:
    static constexpr uint8_t INVALID_NODE = 0;
    static constexpr uint8_t SKEY_DELETED_NODE = 1;
    static constexpr uint8_t PKEY_DELETED_NODE = 2;
    static constexpr uint8_t VALUE_NODE = 3;
    struct Meta {
        uint8_t status  : 2;
        bool isLastNode : 1;
    } __attribute__((packed));

    static_assert(sizeof(Meta) == 1, "Meta is not one byte");
    static_assert(std::is_trivially_copyable_v<Meta>);

public:
    Meta meta;
    SKeyType skey;

public:
    inline bool IsValidNode() const { return meta.status != INVALID_NODE; }
    inline bool IsLastNode() const { return meta.isLastNode; }
    inline bool IsPKeyDeleted() const { return meta.status == PKEY_DELETED_NODE; }
    inline bool IsSKeyDeleted() const { return meta.status == SKEY_DELETED_NODE; }
    inline bool HasValue() const { return meta.status == VALUE_NODE; }

    static inline InlineOnDiskSKeyNode PKeyDeletedNode(bool isLastNode)
    {
        return {{PKEY_DELETED_NODE, isLastNode}, SKeyType()};
    }
    static inline InlineOnDiskSKeyNode SKeyDeletedNode(SKeyType skey, bool isLastNode)
    {
        return {{SKEY_DELETED_NODE, isLastNode}, skey};
    }
    static inline InlineOnDiskSKeyNode Of(SKeyType skey, bool isLastNode) { return {{VALUE_NODE, isLastNode}, skey}; }

} __attribute__((packed));

} // namespace indexlibv2::index
