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
#ifndef __INDEXLIB_SEGMENT_SYNC_ITEM_H
#define __INDEXLIB_SEGMENT_SYNC_ITEM_H

#include <memory>

#include "autil/Lock.h"
#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class SegmentInfoSynchronizer
{
public:
    SegmentInfoSynchronizer(const index_base::SegmentInfo& segInfo, int64_t segmentFileCount)
        : mSegmentInfo(segInfo)
        , mSegmentFileCount(segmentFileCount)
    {
    }

    void TrySyncSegmentInfo(const file_system::DirectoryPtr& targetSegDir)
    {
        autil::ScopedLock lock(mLock);
        if (--mSegmentFileCount == 1) {
            mSegmentInfo.Store(targetSegDir);
        }
    }

private:
    index_base::SegmentInfo mSegmentInfo;
    int64_t mSegmentFileCount;
    mutable autil::ThreadMutex mLock;
};

DEFINE_SHARED_PTR(SegmentInfoSynchronizer);

class SegmentSyncItem : public autil::WorkItem
{
public:
    const static size_t DEFAULT_BUFFER_SIZE;

public:
    SegmentSyncItem(const SegmentInfoSynchronizerPtr& synchronizer, const file_system::FileInfo& fileInfo,
                    const file_system::DirectoryPtr& sourceSegDir, const file_system::DirectoryPtr& targetSegDir);
    ~SegmentSyncItem();

public:
    void process() override;

private:
    SegmentInfoSynchronizerPtr mSynchronizer;
    file_system::FileInfo mFileInfo;
    file_system::DirectoryPtr mSourceSegDir;
    file_system::DirectoryPtr mTargetSegDir;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentSyncItem);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SEGMENT_SYNC_ITEM_H
