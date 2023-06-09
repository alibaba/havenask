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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/merger/DocumentMergeInfo.h"

namespace indexlibv2::index {
class DocMapper;

class DocumentMergeInfoHeap : private autil::NoCopyable
{
public:
    DocumentMergeInfoHeap();
    ~DocumentMergeInfoHeap();

public:
    void Init(const IIndexMerger::SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper> docMapper);
    bool GetNext(DocumentMergeInfo& docMergeInfo);
    bool IsEmpty() const { return _currentDocInfo.newDocId == INVALID_DOCID; }

    const DocumentMergeInfo& Top() const { return _currentDocInfo; }

    const std::shared_ptr<DocMapper>& GetReclaimMap() const { return _docMapper; }

    DocumentMergeInfoHeap* Clone()
    {
        DocumentMergeInfoHeap* heap = new DocumentMergeInfoHeap;
        heap->Init(_segMergeInfos, _docMapper);
        return heap;
    }

private:
    void LoadNextDoc();

private:
    IIndexMerger::SegmentMergeInfos _segMergeInfos;
    std::shared_ptr<DocMapper> _docMapper;
    std::vector<docid_t> _docIdCursors;

    size_t _segCursor;
    DocumentMergeInfo _currentDocInfo;
    std::map<segmentid_t, docid_t> _nextNewLocalDocIds;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
