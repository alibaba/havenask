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
#include "indexlib/index/inverted_index/SegmentTermInfo.h"

#include "indexlib/index/inverted_index/IndexIterator.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"

namespace indexlib::index {
SegmentTermInfo::SegmentTermInfo()
    : _indexType(it_unknown)
    , _segId(INVALID_SEGMENTID)
    , _baseDocId(INVALID_DOCID)
    , _postingKey(0)
    , _postingDecoder(nullptr)
    , _patchTermKey(0)
    , _termPatchIterator(nullptr)
    , _status(0)
    , _lastReadFrom(BOTH)
    , _termIndexMode(TM_NORMAL)
{
}

SegmentTermInfo::SegmentTermInfo(InvertedIndexType indexType, segmentid_t id, docid_t baseId,
                                 const std::shared_ptr<IndexIterator>& indexIterator,
                                 const std::shared_ptr<SingleFieldIndexSegmentPatchIterator>& fieldPatchIterator,
                                 TermIndexMode mode)
    : _indexType(indexType)
    , _segId(id)
    , _baseDocId(baseId)
    , _indexIterator(indexIterator)
    , _postingKey(0)
    , _postingDecoder(nullptr)
    , _fieldPatchIterator(fieldPatchIterator)
    , _patchTermKey(0)
    , _termPatchIterator(nullptr)
    , _status(0)
    , _lastReadFrom(BOTH)
    , _termIndexMode(mode)
{
}

index::DictKeyInfo SegmentTermInfo::GetKey() const
{
    if (_lastReadFrom == PATCH) {
        return _patchTermKey;
    } else if (_lastReadFrom == POSTING) {
        return _postingKey;
    }
    return _postingKey;
}

std::pair<PostingDecoder*, SingleTermIndexSegmentPatchIterator*> SegmentTermInfo::GetPosting() const
{
    if (_lastReadFrom == PATCH) {
        return {nullptr, _termPatchIterator.get()};
    } else if (_lastReadFrom == POSTING) {
        return {_postingDecoder, nullptr};
    }
    return {_postingDecoder, _termPatchIterator.get()};
}

bool SegmentTermInfo::Next()
{
    if ((!_indexIterator or !_indexIterator->HasNext()) and !_fieldPatchIterator) {
        return false;
    }
    if (_lastReadFrom == PATCH) {
        PatchNext();
    } else if (_lastReadFrom == POSTING) {
        PostingNext();
    } else {
        PatchNext();
        PostingNext();
    }
    if ((_status & HAS_POSTING) and (_status & HAS_PATCH)) {
        if (_postingKey < _patchTermKey) {
            _lastReadFrom = POSTING;
        } else if (_postingKey > _patchTermKey) {
            _lastReadFrom = PATCH;
        } else {
            _lastReadFrom = BOTH;
        }
    } else if (_status & HAS_PATCH) {
        _lastReadFrom = PATCH;
    } else if (_status & HAS_POSTING) {
        _lastReadFrom = POSTING;
    } else {
        return false;
    }
    return true;
}

void SegmentTermInfo::PatchNext()
{
    _status = _status & (~HAS_PATCH);
    if (!_fieldPatchIterator) {
        return;
    }
    std::unique_ptr<SingleTermIndexSegmentPatchIterator> termIter = _fieldPatchIterator->NextTerm();
    if (termIter == nullptr) {
        _fieldPatchIterator = nullptr;
    } else {
        _termPatchIterator = std::move(termIter);
        _patchTermKey = TransformPatchKey(_termPatchIterator->GetTermKey());
        _status |= HAS_PATCH;
    }
}

index::DictKeyInfo SegmentTermInfo::TransformPatchKey(index::DictKeyInfo termKey)
{
    if (termKey.IsNull()) {
        return termKey;
    }
    dictkey_t dictKey = 0;
    switch (_indexType) {
    case it_number_int32:
    case it_number_uint32:
        dictKey = static_cast<uint32_t>(termKey.GetKey());
        break;
    case it_number_int64:
    case it_number_uint64:
        dictKey = static_cast<uint64_t>(termKey.GetKey());
        break;
    case it_number_int8:
    case it_number_uint8:
        dictKey = static_cast<uint8_t>(termKey.GetKey());
        break;
    case it_number_int16:
    case it_number_uint16:
        dictKey = static_cast<uint16_t>(termKey.GetKey());
        break;
    default:
        dictKey = termKey.GetKey();
        break;
    }
    return index::DictKeyInfo(dictKey);
}

void SegmentTermInfo::PostingNext()
{
    _status = _status & (~HAS_POSTING);
    if (!_indexIterator) {
        return;
    }
    if (_indexIterator->HasNext()) {
        _postingDecoder = _indexIterator->Next(_postingKey);
        _status |= HAS_POSTING;
    }
}

bool SegmentTermInfoComparator::operator()(const SegmentTermInfo* item1, const SegmentTermInfo* item2)
{
    if (item1->GetKey() != item2->GetKey()) {
        return item1->GetKey() > item2->GetKey();
    }

    if (item1->GetTermIndexMode() != item2->GetTermIndexMode()) {
        return item1->GetTermIndexMode() > item2->GetTermIndexMode();
    }
    return item1->GetSegmentId() > item2->GetSegmentId();
}
} // namespace indexlib::index
