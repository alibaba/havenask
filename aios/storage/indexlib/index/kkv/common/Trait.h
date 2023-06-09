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
#include "indexlib/index/kkv/common/InlineOnDiskSKeyNode.h"
#include "indexlib/index/kkv/common/NormalOnDiskSKeyNode.h"

namespace indexlibv2::index {

template <typename SKeyType, bool valueInline>
struct SKeyNodeTraits {
};

template <typename SKeyType>
struct SKeyNodeTraits<SKeyType, true> {
    using SKeyNode = InlineOnDiskSKeyNode<SKeyType>;
};

template <typename SKeyType>
struct SKeyNodeTraits<SKeyType, false> {
    using SKeyNode = NormalOnDiskSKeyNode<SKeyType>;
};

#define KKV_SKEY_DICT_TYPE_MACRO_HELPER(MACRO)                                                                         \
    MACRO(ft_int32);                                                                                                   \
    MACRO(ft_int64);                                                                                                   \
    MACRO(ft_uint32);                                                                                                  \
    MACRO(ft_uint64);                                                                                                  \
    MACRO(ft_int8);                                                                                                    \
    MACRO(ft_uint8);                                                                                                   \
    MACRO(ft_int16);                                                                                                   \
    MACRO(ft_uint16);

#define EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(T)                                                               \
    template class T<int8_t>;                                                                                          \
    template class T<int16_t>;                                                                                         \
    template class T<int32_t>;                                                                                         \
    template class T<int64_t>;                                                                                         \
    template class T<uint8_t>;                                                                                         \
    template class T<uint16_t>;                                                                                        \
    template class T<uint32_t>;                                                                                        \
    template class T<uint64_t>;

#define MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(T)                                                          \
    extern template class T<int8_t>;                                                                                   \
    extern template class T<int16_t>;                                                                                  \
    extern template class T<int32_t>;                                                                                  \
    extern template class T<int64_t>;                                                                                  \
    extern template class T<uint8_t>;                                                                                  \
    extern template class T<uint16_t>;                                                                                 \
    extern template class T<uint32_t>;                                                                                 \
    extern template class T<uint64_t>;

} // namespace indexlibv2::index
