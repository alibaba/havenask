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
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class BuildingSegmentIterator : public SegmentIterator
{
public:
    BuildingSegmentIterator();
    ~BuildingSegmentIterator();

public:
    void Init(const std::vector<InMemorySegmentPtr>& inMemSegments, exdocid_t baseDocId);

    bool IsValid() const override;

    void MoveToNext() override;

    InMemorySegmentPtr GetInMemSegment() const override;

    SegmentData GetSegmentData() const override;

    exdocid_t GetBaseDocId() const override;

    segmentid_t GetSegmentId() const override;

    SegmentIteratorType GetType() const override;

    void Reset() override;

private:
    std::vector<InMemorySegmentPtr> mInMemSegments;
    exdocid_t mCurrentBaseDocId;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSegmentIterator);
}} // namespace indexlib::index_base
