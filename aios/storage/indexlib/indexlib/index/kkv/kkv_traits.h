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
#ifndef __INDEXLIB_KKV_TRAITS_H
#define __INDEXLIB_KKV_TRAITS_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType, bool valueInline>
struct SuffixKeyTraits {
};

template <typename SKeyType>
struct SuffixKeyTraits<SKeyType, true> {
    using SKeyNode = ValueInlineOnDiskSKeyNode<SKeyType>;
};

template <typename SKeyType>
struct SuffixKeyTraits<SKeyType, false> {
    using SKeyNode = NormalOnDiskSKeyNode<SKeyType>;
};
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_TRAITS_H
