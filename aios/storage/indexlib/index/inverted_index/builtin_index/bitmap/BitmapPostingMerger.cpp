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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingMerger.h"

#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDecoder.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"

namespace indexlib::index {
namespace {
using indexlibv2::index::DocMapper;
}

AUTIL_LOG_SETUP(indexlib.index, BitmapPostingMerger);

class BitmapIndexSortedQueueItem
{
public:
    BitmapIndexSortedQueueItem(const SegmentTermInfo& segTermInfo, const std::shared_ptr<DocMapper>& docMapper)
        : _docMapper(docMapper)
        , _baseDocId(segTermInfo.GetBaseDocId())
    {
        std::pair<PostingDecoder*, SingleTermIndexSegmentPatchIterator*> pair = segTermInfo.GetPosting();
        _bitmapDecoder = dynamic_cast<BitmapPostingDecoder*>(pair.first);
    }
    bool Next()
    {
        if (_end) {
            return false;
        }

        if (MoveNextInDocBuffer()) {
            return true;
        }

        if (!_bitmapDecoder) {
            return false;
        }
        while (1) {
            _decodedNum = _bitmapDecoder->DecodeDocList(_docBuffer, DEFAULT_BUFFER_SIZE);
            if (_decodedNum <= 0) {
                _end = true;
                return false;
            }

            _docCursor = 0;
            if (MoveNextInDocBuffer()) {
                return true;
            }
        }
    }
    std::pair<segmentid_t, docid_t> GetCurrent() const { return _current; }
    docid_t GetOldDocId() const { return _oldDocId; }
    const TermMeta* GetTermMeta() const { return _bitmapDecoder->GetTermMeta(); }

private:
    bool MoveNextInDocBuffer()
    {
        while (_docCursor < (uint32_t)_decodedNum) {
            _oldDocId = _docBuffer[_docCursor] + _baseDocId;
            _current = _docMapper->Map(_oldDocId);
            _docCursor++;
            if (_current.second != INVALID_DOCID) {
                return true;
            }
        }
        return false;
    }

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 8;

    BitmapPostingDecoder* _bitmapDecoder = nullptr;
    std::shared_ptr<DocMapper> _docMapper;
    docid_t _baseDocId = INVALID_DOCID;
    docid_t _oldDocId = INVALID_DOCID;
    std::pair<segmentid_t, docid_t> _current = {INVALID_SEGMENTID, INVALID_DOCID};

    docid_t _docBuffer[DEFAULT_BUFFER_SIZE];
    int32_t _decodedNum = 0;
    uint32_t _docCursor = 0;
    bool _end = false;
};

struct BitmapIndexSortedQueueItemComparator {
    bool operator()(const BitmapIndexSortedQueueItem* item1, const BitmapIndexSortedQueueItem* item2)
    {
        return (item1->GetCurrent() > item2->GetCurrent());
    }
};

using PriorityQueue = std::priority_queue<BitmapIndexSortedQueueItem*, std::vector<BitmapIndexSortedQueueItem*>,
                                          BitmapIndexSortedQueueItemComparator>;

BitmapPostingMerger::BitmapPostingMerger(
    autil::mem_pool::Pool* pool, const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments,
    optionflag_t optionFlag)
    : _pool(pool)
    , _writer(pool, targetSegments)
    , _df(-1)
    , _optionFlag(optionFlag)
{
    (void)_pool;
}

BitmapPostingMerger::~BitmapPostingMerger() {}

void BitmapPostingMerger::ApplyPatch(const SegmentTermInfo* segTermInfo, const std::shared_ptr<DocMapper>& docMapper)
{
    auto pair = segTermInfo->GetPosting();
    SingleTermIndexSegmentPatchIterator* patchIter = pair.second;
    if (patchIter) {
        docid_t baseDocId = segTermInfo->GetBaseDocId();
        ComplexDocId doc;
        while (patchIter->Next(&doc)) {
            docid_t oldId = baseDocId + doc.DocId();
            auto [newSegId, newLocalDocId] = docMapper->Map(oldId);
            if (newLocalDocId != INVALID_DOCID) {
                auto postingWriter = _writer.GetSegmentBitmapPostingWriterBySegId(newSegId);
                static_cast<BitmapPostingWriter*>(postingWriter.get())->Update(newLocalDocId, doc.IsDelete());
            }
        }
    }
}

void BitmapPostingMerger::Merge(const SegmentTermInfos& segTermInfos, const std::shared_ptr<DocMapper>& docMapper)
{
    PriorityQueue priorityQueue;
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        BitmapIndexSortedQueueItem* queueItem = new BitmapIndexSortedQueueItem(*(segTermInfos[i]), docMapper);
        if (queueItem->Next()) {
            priorityQueue.push(queueItem);
            _termPayload = queueItem->GetTermMeta()->GetPayload();
        } else {
            delete queueItem;
        }
    }

    while (!priorityQueue.empty()) {
        BitmapIndexSortedQueueItem* item = priorityQueue.top();
        priorityQueue.pop();
        // old global docid
        docid_t oldDocId = item->GetOldDocId();
        auto [newSegId, newLocalDocId] = docMapper->Map(oldDocId);
        auto postingWriter = _writer.GetSegmentBitmapPostingWriterBySegId(newSegId);
        postingWriter->EndDocument(newLocalDocId, 0);

        if (item->Next()) {
            priorityQueue.push(item);
        } else {
            delete item;
        }
    }
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        ApplyPatch(segTermInfos[i], docMapper);
    }
}

void BitmapPostingMerger::Dump(
    const index::DictKeyInfo& key,
    const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources)
{
    _writer.Dump(key, indexOutputSegmentResources, _termPayload);
}

} // namespace indexlib::index
