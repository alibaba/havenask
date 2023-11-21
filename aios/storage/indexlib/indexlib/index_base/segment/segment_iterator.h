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
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

namespace indexlib { namespace index_base {

enum SegmentIteratorType {
    SIT_BUILDING,
    SIT_BUILT,
};

class SegmentIterator
{
public:
    SegmentIterator() {}
    virtual ~SegmentIterator() {}

public:
    virtual bool IsValid() const = 0;
    virtual void MoveToNext() = 0;
    virtual InMemorySegmentPtr GetInMemSegment() const = 0;
    virtual SegmentData GetSegmentData() const = 0;
    virtual exdocid_t GetBaseDocId() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    virtual SegmentIteratorType GetType() const = 0;
    virtual void Reset() = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentIterator);
}} // namespace indexlib::index_base
