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
#ifndef __INDEXLIB_SEGMENT_UPDATE_BITMAP_H
#define __INDEXLIB_SEGMENT_UPDATE_BITMAP_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class SegmentUpdateBitmap
{
public:
    SegmentUpdateBitmap(size_t docCount, autil::mem_pool::PoolBase* pool = NULL)
        : mDocCount(docCount)
        , mBitmap(false, pool)
    {
    }

    ~SegmentUpdateBitmap() {}

public:
    void Set(docid_t localDocId)
    {
        if (unlikely(size_t(localDocId) >= mDocCount)) {
            INDEXLIB_THROW(util::OutOfRangeException, "local docId[%d] out of range[0:%lu)", localDocId, mDocCount);
        }
        if (unlikely(mBitmap.GetItemCount() == 0)) {
            mBitmap.Alloc(mDocCount, false);
        }

        mBitmap.Set(localDocId);
    }

    uint32_t GetUpdateDocCount() const { return mBitmap.GetSetCount(); }

private:
    size_t mDocCount;
    util::Bitmap mBitmap;
};

DEFINE_SHARED_PTR(SegmentUpdateBitmap);
}} // namespace indexlib::index

#endif //__INDEXLIB_SEGMENT_UPDATE_BITMAP_H
