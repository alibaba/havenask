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
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_merger.h"

#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_sorted_queue_item.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, BitmapPostingMerger);

BitmapPostingMerger::BitmapPostingMerger(Pool* pool, const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                                         optionflag_t optionFlag)
    : mPool(pool)
    , mWriter(pool, outputSegmentMergeInfos)
    , mDf(-1)
    , mOptionFlag(optionFlag)
{
    (void)mPool;
}

BitmapPostingMerger::~BitmapPostingMerger() {}

void BitmapPostingMerger::Merge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap)
{
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        const SegmentTermInfo* segTermInfo = segTermInfos[i];
        auto pair = segTermInfo->GetPosting();
        BitmapPostingDecoder* postDecoder = dynamic_cast<BitmapPostingDecoder*>(pair.first);

        mTermPayload = 0;
        if (postDecoder) {
            const TermMeta* termMeta = postDecoder->GetTermMeta();
            mTermPayload = termMeta->GetPayload();
        }

        docid_t baseDocId = segTermInfo->GetBaseDocId();
        docid_t docBuffer[DEFAULT_BUFFER_SIZE];
        int32_t decodedNum;

        if (postDecoder) {
            while (1) {
                decodedNum = postDecoder->DecodeDocList(docBuffer, DEFAULT_BUFFER_SIZE);
                if (decodedNum <= 0) {
                    break;
                }
                for (uint32_t i = 0; i < (uint32_t)decodedNum; ++i) {
                    docid_t oldId = baseDocId + docBuffer[i];
                    docid_t newId = reclaimMap->GetNewId(oldId);
                    if (newId != INVALID_DOCID) {
                        std::pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetLocalId(newId);
                        auto postingWriter = mWriter.GetSegmentBitmapPostingWriter(docidInfo.first);
                        postingWriter->EndDocument(docidInfo.second, 0);
                    }
                }
            }
        }
        ApplyPatch(segTermInfos[i], reclaimMap);
    }
}

void BitmapPostingMerger::ApplyPatch(const SegmentTermInfo* segTermInfo, const ReclaimMapPtr& reclaimMap)
{
    auto pair = segTermInfo->GetPosting();
    SingleTermIndexSegmentPatchIterator* patchIter = pair.second;
    if (patchIter) {
        docid_t baseDocId = segTermInfo->GetBaseDocId();
        ComplexDocId doc;
        while (patchIter->Next(&doc)) {
            docid_t oldId = baseDocId + doc.DocId();
            docid_t newId = reclaimMap->GetNewId(oldId);
            if (newId != INVALID_DOCID) {
                std::pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetLocalId(newId);
                auto postingWriter = mWriter.GetSegmentBitmapPostingWriter(docidInfo.first);
                static_cast<BitmapPostingWriter*>(postingWriter.get())->Update(docidInfo.second, doc.IsDelete());
            }
        }
    }
}

void BitmapPostingMerger::SortByWeightMerge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap)
{
    SortByWeightQueue sortByWeightQueue;
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        BitmapIndexSortedQueueItem* queueItem = new BitmapIndexSortedQueueItem(*(segTermInfos[i]), reclaimMap);
        if (queueItem->Next()) {
            sortByWeightQueue.push(queueItem);
            mTermPayload = queueItem->GetTermMeta()->GetPayload();
        } else {
            delete queueItem;
        }
    }

    while (!sortByWeightQueue.empty()) {
        BitmapIndexSortedQueueItem* item = sortByWeightQueue.top();
        sortByWeightQueue.pop();
        docid_t newId = item->GetDocId();
        pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetLocalId(newId);
        auto postingWriter = mWriter.GetSegmentBitmapPostingWriter(docidInfo.first);
        postingWriter->EndDocument(docidInfo.second, 0);

        if (item->Next()) {
            sortByWeightQueue.push(item);
        } else {
            delete item;
        }
    }
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        ApplyPatch(segTermInfos[i], reclaimMap);
    }
}

void BitmapPostingMerger::Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources)
{
    mWriter.Dump(key, indexOutputSegmentResources, mTermPayload);
}
}}} // namespace indexlib::index::legacy
