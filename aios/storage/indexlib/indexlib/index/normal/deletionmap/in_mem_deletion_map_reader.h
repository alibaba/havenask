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
#ifndef __INDEXLIB_IN_MEM_DELETION_MAP_READER_H
#define __INDEXLIB_IN_MEM_DELETION_MAP_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ExpandableBitmap.h"

namespace indexlib { namespace index {

class InMemDeletionMapReader
{
public:
    InMemDeletionMapReader(util::ExpandableBitmap* bitmap, index_base::SegmentInfo* segmentInfo, segmentid_t segId);
    ~InMemDeletionMapReader();

private:
    InMemDeletionMapReader(const InMemDeletionMapReader& bitmap);

public:
    bool IsDeleted(docid_t docId) const
    {
        if (docId >= (docid_t)mSegmentInfo->docCount) {
            return true;
        }
        uint32_t currentDocCount = mBitmap->GetValidItemCount();
        if (docId >= (docid_t)currentDocCount) {
            return false;
        }
        return mBitmap->Test(docId);
    }

    void Delete(docid_t docId) { mBitmap->Set(docId); }

    uint32_t GetDeletedDocCount() const { return mBitmap->GetSetCount(); }

    segmentid_t GetInMemSegmentId() const { return mSegId; }

    util::ExpandableBitmap* GetBitmap() const { return mBitmap; }

private:
    util::ExpandableBitmap* mBitmap;
    index_base::SegmentInfo* mSegmentInfo;
    segmentid_t mSegId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemDeletionMapReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_IN_MEM_DELETION_MAP_READER_H
