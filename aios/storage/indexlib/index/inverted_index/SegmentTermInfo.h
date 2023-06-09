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

#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/IndexIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"

namespace indexlib::index {
class PostingDecoder;
class IndexIterator;
class IndexUpdateTermIterator;
class SingleFieldIndexSegmentPatchIterator;
class SegmentTermInfo
{
private:
    enum ReadFrom {
        PATCH,
        POSTING,
        BOTH,
    };
    enum StatusBit : uint8_t {
        HAS_POSTING = 0x1,
        HAS_PATCH = 0x2,
    };

public:
    enum TermIndexMode { TM_NORMAL = 0, TM_BITMAP = 1 };

public:
    SegmentTermInfo();
    SegmentTermInfo(InvertedIndexType indexType, segmentid_t id, docid_t baseId,
                    const std::shared_ptr<IndexIterator>& indexIterator,
                    const std::shared_ptr<SingleFieldIndexSegmentPatchIterator>& fieldPatchIterator,
                    TermIndexMode mode = TM_NORMAL);

    segmentid_t GetSegmentId() const { return _segId; }
    index::DictKeyInfo GetKey() const;
    docid_t GetBaseDocId() const { return _baseDocId; }
    std::pair<PostingDecoder*, SingleTermIndexSegmentPatchIterator*> GetPosting() const;
    TermIndexMode GetTermIndexMode() const { return _termIndexMode; }

    bool Next();

private:
    void PatchNext();
    void PostingNext();
    index::DictKeyInfo TransformPatchKey(index::DictKeyInfo termKey);

private:
    InvertedIndexType _indexType;
    segmentid_t _segId;
    docid_t _baseDocId;

    std::shared_ptr<IndexIterator> _indexIterator;
    index::DictKeyInfo _postingKey;
    PostingDecoder* _postingDecoder;

    std::shared_ptr<SingleFieldIndexSegmentPatchIterator> _fieldPatchIterator;
    index::DictKeyInfo _patchTermKey;
    std::unique_ptr<SingleTermIndexSegmentPatchIterator> _termPatchIterator;

    uint8_t _status;
    ReadFrom _lastReadFrom;
    TermIndexMode _termIndexMode;

private:
    friend class SegmentTermInfoTest;
};

class SegmentTermInfoComparator
{
public:
    bool operator()(const SegmentTermInfo* item1, const SegmentTermInfo* item2);
};

typedef std::vector<SegmentTermInfo*> SegmentTermInfos;
} // namespace indexlib::index
