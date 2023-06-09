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

#include <cassert>
#include <cstdint>

#include "indexlib/index/kkv/Constant.h"

namespace indexlibv2::index {

class KKVTTLHelper
{
public:
    static bool SKeyExpired(uint64_t currentTs, uint64_t docTs, uint64_t docExpireTime, int64_t defaultTTL)
    {
        if (docExpireTime == indexlib::UNINITIALIZED_EXPIRE_TIME) {
            assert(defaultTTL > 0);
            docExpireTime = docTs + defaultTTL;
        }
        return docExpireTime < currentTs;
    }
};

} // namespace indexlibv2::index
