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
#include "indexlib/util/KeyHasherTyped.h"

namespace indexlib { namespace common {

class MultiRegionRehasher
{
public:
    MultiRegionRehasher() {}
    ~MultiRegionRehasher() {}

public:
    static inline dictkey_t GetRehashKey(dictkey_t keyHash, regionid_t regionId)
    {
        autil::StringView rehashKeyStr((char*)&keyHash, sizeof(dictkey_t));
        dictkey_t rehashKey = 0;
        util::MurmurHasher::GetHashKey(rehashKeyStr, rehashKey, regionId);
        return rehashKey;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionRehasher);
}} // namespace indexlib::common
