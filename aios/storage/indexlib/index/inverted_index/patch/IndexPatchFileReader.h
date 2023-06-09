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
#include <queue>

#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"
#include "indexlib/index/inverted_index/format/PatchFormat.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

class IndexPatchFileReader
{
private:
    static constexpr double PATCH_EXPAND_FACTOR = 1.2;

public:
    IndexPatchFileReader(segmentid_t srcSegmentId, segmentid_t targetSegmentId, bool patchCompress);
    Status Open(const std::shared_ptr<file_system::IDirectory>& directory, const std::string& fileName);
    bool HasNext() const;
    void Next();

    index::DictKeyInfo GetCurrentTermKey() const { return _currentTermKey; }
    ComplexDocId GetCurrentDocId() const { return _currentDocId; }

    void SkipCurrentTerm();
    segmentid_t GetSrcSegmentId() const { return _srcSegmentId; }
    segmentid_t GetTargetSegmentId() const { return _targetSegmentId; }
    size_t GetPatchLoadExpandSize() const { return _dataLength * PATCH_EXPAND_FACTOR; }
    size_t GetPatchItemCount() const { return _patchItemCount; }

private:
    file_system::FileReaderPtr _fileReader;
    PatchFormat::PatchMeta _meta;
    size_t _dataLength;
    segmentid_t _srcSegmentId;
    segmentid_t _targetSegmentId;
    bool _patchCompressed;
    int64_t _cursor;
    uint32_t _curLeftDocs;
    size_t _leftNonNullTermCount;
    size_t _patchItemCount;
    index::DictKeyInfo _currentTermKey;
    ComplexDocId _currentDocId;

private:
    AUTIL_LOG_DECLARE();
};

class IndexPatchFileReaderComparator
{
public:
    bool operator()(IndexPatchFileReader* lhs, IndexPatchFileReader* rhs) const
    {
        if (lhs->GetCurrentTermKey() != rhs->GetCurrentTermKey()) {
            return lhs->GetCurrentTermKey() > rhs->GetCurrentTermKey();
        }
        if (lhs->GetCurrentDocId() != rhs->GetCurrentDocId()) {
            return lhs->GetCurrentDocId() > rhs->GetCurrentDocId();
        }
        return lhs->GetSrcSegmentId() < rhs->GetSrcSegmentId();
    }
};

typedef std::priority_queue<IndexPatchFileReader*, std::vector<IndexPatchFileReader*>, IndexPatchFileReaderComparator>
    IndexPatchHeap;

} // namespace indexlib::index
