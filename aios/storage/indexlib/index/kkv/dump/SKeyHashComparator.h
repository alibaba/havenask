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

#include "indexlib/index/kkv/dump/SKeyCollectInfo.h"

namespace indexlibv2::index {

template <typename SKeyType>
class SKeyHashComparator
{
public:
    bool operator()(SKeyCollectInfo*& lft, SKeyCollectInfo*& rht)
    {
        assert(!lft->isDeletedPKey && !rht->isDeletedPKey);
        return (SKeyType)lft->skey < (SKeyType)rht->skey;
    }
};

} // namespace indexlibv2::index
