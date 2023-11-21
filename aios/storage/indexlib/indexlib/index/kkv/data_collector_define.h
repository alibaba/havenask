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
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

struct SKeyCollectInfo {
    SKeyCollectInfo()
        : skey(0)
        , ts(0)
        , expireTime(UNINITIALIZED_EXPIRE_TIME)
        , isDeletedPKey(false)
        , isDeletedSKey(false)
        , value(autil::StringView::empty_instance())
    {
    }

    uint64_t skey;
    uint32_t ts;
    uint32_t expireTime;
    bool isDeletedPKey;
    bool isDeletedSKey;
    autil::StringView value;
};

class SKeyCollectInfoPool
{
public:
    SKeyCollectInfoPool() : mCursor(0) {}

    ~SKeyCollectInfoPool()
    {
        for (size_t i = 0; i < mCollectInfoPool.size(); ++i) {
            assert(mCollectInfoPool[i]);
            IE_POOL_COMPATIBLE_DELETE_CLASS((&mPool), mCollectInfoPool[i]);
            mCollectInfoPool[i] = NULL;
        }
    }

    SKeyCollectInfo* CreateCollectInfo()
    {
        while (mCollectInfoPool.size() <= mCursor) {
            SKeyCollectInfo* info = IE_POOL_COMPATIBLE_NEW_CLASS((&mPool), SKeyCollectInfo);
            mCollectInfoPool.push_back(info);
        }
        return mCollectInfoPool[mCursor++];
    }

    void Reset() { mCursor = 0; }

private:
    std::vector<SKeyCollectInfo*> mCollectInfoPool;
    size_t mCursor;
    autil::mem_pool::Pool mPool;
};
}} // namespace indexlib::index
