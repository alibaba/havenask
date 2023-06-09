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
#include <vector>

#include "indexlib/base/Constant.h"

namespace indexlib::index {

class OperationRedoHint
{
public:
    enum HintType { REDO_USE_HINT_DOC, REDO_CACHE_SEGMENT, REDO_DOC_RANGE, REDO_UNKNOWN };

public:
    OperationRedoHint() { Reset(); }
    ~OperationRedoHint() {}

public:
    bool IsValid() const;
    void Reset();

    void SetSegmentId(segmentid_t segId);
    segmentid_t GetSegmentId() const { return _segId; }
    void SetLocalDocId(docid_t localDocId);
    docid_t GetLocalDocId() const { return _docId; }
    void SetHintType(HintType hintType) { _hintType = hintType; }
    HintType GetHintType() const { return _hintType; }
    void AddCachedSegment(segmentid_t segId) { _cachedSegments.push_back(segId); }
    const std::vector<segmentid_t>& GetCachedSegmentIds() const { return _cachedSegments; }
    void SetCachedDocRanges(const std::vector<std::pair<docid_t, docid_t>>& docRanges,
                            const std::vector<std::pair<docid_t, docid_t>>& subDocRanges);
    std::pair<std::vector<std::pair<docid_t, docid_t>>, std::vector<std::pair<docid_t, docid_t>>>
    GetCachedDocRange() const;

private:
    segmentid_t _segId;
    docid_t _docId;
    HintType _hintType;
    std::vector<segmentid_t> _cachedSegments;
    std::vector<std::pair<docid_t, docid_t>> _cachedDocRange;
    std::vector<std::pair<docid_t, docid_t>> _subCachedDocRange;
};

typedef std::shared_ptr<OperationRedoHint> OperationRedoHintPtr;

inline bool OperationRedoHint::IsValid() const
{
    return (_segId != INVALID_SEGMENTID && _docId != INVALID_DOCID && _hintType == REDO_USE_HINT_DOC) ||
           _hintType == REDO_CACHE_SEGMENT || _hintType == REDO_DOC_RANGE;
}

inline void OperationRedoHint::Reset()
{
    _segId = INVALID_SEGMENTID;
    _docId = INVALID_DOCID;
    _hintType = REDO_UNKNOWN;
    _cachedSegments.clear();
    _cachedDocRange.clear();
    _subCachedDocRange.clear();
}
inline void OperationRedoHint::SetSegmentId(segmentid_t segId)
{
    _segId = segId;
    _hintType = REDO_USE_HINT_DOC;
}
inline void OperationRedoHint::SetLocalDocId(docid_t localDocId)
{
    _docId = localDocId;
    _hintType = REDO_USE_HINT_DOC;
};
inline void OperationRedoHint::SetCachedDocRanges(const std::vector<std::pair<docid_t, docid_t>>& docRanges,
                                                  const std::vector<std::pair<docid_t, docid_t>>& subDocRanges)
{
    _cachedDocRange = docRanges;
    _subCachedDocRange = subDocRanges;
}
inline std::pair<std::vector<std::pair<docid_t, docid_t>>, std::vector<std::pair<docid_t, docid_t>>>
OperationRedoHint::GetCachedDocRange() const
{
    return std::make_pair(_cachedDocRange, _subCachedDocRange);
}

} // namespace indexlib::index
