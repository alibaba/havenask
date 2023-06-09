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
#include "indexlib/index/attribute/merger/DocumentMergeInfoHeap.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/DocMapper.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DocumentMergeInfoHeap);

DocumentMergeInfoHeap::DocumentMergeInfoHeap() : _segCursor(0) {}

DocumentMergeInfoHeap::~DocumentMergeInfoHeap() {}

void DocumentMergeInfoHeap::Init(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                 const std::shared_ptr<DocMapper> docMapper)
{
    _segMergeInfos = segMergeInfos;
    _docMapper = docMapper;
    _docIdCursors.resize(_segMergeInfos.srcSegments.size(), 0);
    for (const auto& segMeta : _segMergeInfos.targetSegments) {
        _nextNewLocalDocIds[segMeta->segmentId] = 0;
    }

    LoadNextDoc();
}

void DocumentMergeInfoHeap::LoadNextDoc()
{
    auto& srcSegments = _segMergeInfos.srcSegments;
    uint32_t segCount = srcSegments.size();
    size_t i = 0;
    for (; i < segCount; ++i) {
        while (_docIdCursors[_segCursor] < (docid_t)(srcSegments[_segCursor].segment->GetSegmentInfo()->docCount)) {
            auto [newSegId, newId] = _docMapper->Map(_docIdCursors[_segCursor] + srcSegments[_segCursor].baseDocid);
            if (newId != INVALID_DOCID && newSegId != INVALID_SEGMENTID) {
                if (newId == _nextNewLocalDocIds[newSegId]) {
                    _currentDocInfo.segmentIndex = _segCursor;
                    _currentDocInfo.oldDocId = _docIdCursors[_segCursor] + srcSegments[_segCursor].baseDocid;
                    _currentDocInfo.newDocId = newId;
                    _currentDocInfo.targetSegmentId = newSegId;
                    return;
                }
                break;
            } else {
                _docIdCursors[_segCursor]++;
            }
        }
        ++_segCursor;
        if (_segCursor >= segCount) {
            _segCursor = 0;
        }
    }
    if (i >= segCount) {
        _currentDocInfo = DocumentMergeInfo();
    }
}

bool DocumentMergeInfoHeap::GetNext(DocumentMergeInfo& docMergeInfo)
{
    if (IsEmpty()) {
        return false;
    }

    docMergeInfo = _currentDocInfo;
    ++_nextNewLocalDocIds[docMergeInfo.targetSegmentId];
    _docIdCursors[_segCursor]++;
    LoadNextDoc();

    return true;
}
} // namespace indexlibv2::index
