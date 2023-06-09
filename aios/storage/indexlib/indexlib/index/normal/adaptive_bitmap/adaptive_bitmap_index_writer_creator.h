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
#ifndef __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_CREATOR_H
#define __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger_creator.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {
class MultiAdaptiveBitmapIndexWriter;

class AdaptiveBitmapIndexWriterCreator
{
public:
    AdaptiveBitmapIndexWriterCreator(const file_system::ArchiveFolderPtr& adaptiveDictFolder,
                                     const ReclaimMapPtr& reclaimMap,
                                     const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    virtual ~AdaptiveBitmapIndexWriterCreator();

public:
    std::shared_ptr<MultiAdaptiveBitmapIndexWriter> Create(const config::IndexConfigPtr& indexConf,
                                                           const config::MergeIOConfig& ioConfig);

private:
    // virtual for test
    virtual std::shared_ptr<AdaptiveBitmapTrigger> CreateAdaptiveBitmapTrigger(const config::IndexConfigPtr& indexConf);

private:
    file_system::ArchiveFolderPtr mAdaptiveDictFolder;
    AdaptiveBitmapTriggerCreator mTriggerCreator;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveBitmapIndexWriterCreator);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_CREATOR_H
