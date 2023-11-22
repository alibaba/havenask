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
#include "indexlib/index/inverted_index/IndexDataWriter.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class MultiAdaptiveBitmapIndexWriter
{
public:
    MultiAdaptiveBitmapIndexWriter(const std::shared_ptr<AdaptiveBitmapTrigger>& trigger,
                                   const config::IndexConfigPtr& indexConf,
                                   const file_system::ArchiveFolderPtr& adaptiveDictFolder,
                                   const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
        : mAdaptiveDictFolder(adaptiveDictFolder)
        , mIndexConfig(indexConf)
    {
        for (size_t i = 0; i < outputSegmentMergeInfos.size(); i++) {
            std::shared_ptr<AdaptiveBitmapIndexWriter> adaptiveBitmapIndexWriter(
                new AdaptiveBitmapIndexWriter(trigger, indexConf, adaptiveDictFolder));

            mAdaptiveBitmapIndexWriters.push_back(adaptiveBitmapIndexWriter);
        }
    }
    virtual ~MultiAdaptiveBitmapIndexWriter();

public:
    void Init(IndexOutputSegmentResources& indexOutputSegmentResources)
    {
        assert(indexOutputSegmentResources.size() == mAdaptiveBitmapIndexWriters.size());
        for (size_t i = 0; i < indexOutputSegmentResources.size(); i++) {
            std::shared_ptr<IndexDataWriter> bitmapIndexDataWriter =
                indexOutputSegmentResources[i]->GetIndexDataWriter(SegmentTermInfo::TM_BITMAP);

            mAdaptiveBitmapIndexWriters[i]->Init(bitmapIndexDataWriter->dictWriter,
                                                 bitmapIndexDataWriter->postingWriter);
        }
    }

    void EndPosting();

    bool NeedAdaptiveBitmap(const index::DictKeyInfo& dictKey, const PostingMergerPtr& postingMerger)
    {
        return mAdaptiveBitmapIndexWriters[0]->NeedAdaptiveBitmap(dictKey, postingMerger);
    }

    virtual void AddPosting(const index::DictKeyInfo& dictKey, termpayload_t termPayload,
                            const std::shared_ptr<PostingIterator>& postingIt)
    {
        std::shared_ptr<MultiSegmentPostingIterator> multiIter =
            DYNAMIC_POINTER_CAST(MultiSegmentPostingIterator, postingIt);
        for (size_t i = 0; i < multiIter->GetSegmentPostingCount(); i++) {
            segmentid_t segmentIdx;
            auto postingIter = multiIter->GetSegmentPostingIter(i, segmentIdx);
            mAdaptiveBitmapIndexWriters[segmentIdx]->AddPosting(dictKey, termPayload, postingIter);
        }
    }

    int64_t EstimateMemoryUse(uint32_t docCount) const
    {
        int64_t size = sizeof(AdaptiveBitmapIndexWriter) * mAdaptiveBitmapIndexWriters.size();
        size += docCount * sizeof(docid_t) * 2;
        size +=
            file_system::WriterOption::DEFAULT_BUFFER_SIZE * 2 * mAdaptiveBitmapIndexWriters.size(); // data and dict
        return size;
    }

public:
    std::shared_ptr<AdaptiveBitmapIndexWriter> GetSingleAdaptiveBitmapWriter(uint32_t idx)
    {
        return mAdaptiveBitmapIndexWriters[idx];
    }

private:
    std::vector<std::shared_ptr<AdaptiveBitmapIndexWriter>> mAdaptiveBitmapIndexWriters;
    file_system::ArchiveFolderPtr mAdaptiveDictFolder;
    config::IndexConfigPtr mIndexConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiAdaptiveBitmapIndexWriter);
}}} // namespace indexlib::index::legacy
