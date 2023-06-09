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

#include "autil/Log.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib { namespace util {

class ExpandableBitmap : public Bitmap
{
public:
    ExpandableBitmap(bool bSet = false, autil::mem_pool::PoolBase* pool = NULL);
    ExpandableBitmap(uint32_t nItemCount, bool bSet = false, autil::mem_pool::PoolBase* pool = NULL);
    ExpandableBitmap(const ExpandableBitmap& rhs);
    virtual ~ExpandableBitmap() {}

public:
    bool Set(uint32_t nIndex);
    bool Reset(uint32_t nIndex);
    void ReSize(uint32_t nItemCount);
    void Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete = true);
    void MountWithoutRefreshSetCount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete = true);
    uint32_t GetValidItemCount() const override { return _validItemCount; }

    // return ExpandableBitmap use independent memory, use different pool
    ExpandableBitmap* CloneWithOnlyValidData() const;

    ExpandableBitmap* Clone() const;

    void Expand(uint32_t nIndex);

private:
    uint32_t _validItemCount;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ExpandableBitmap> ExpandableBitmapPtr;
}} // namespace indexlib::util
