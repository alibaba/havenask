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
#include "indexlib/index_base/segment/segment_data_base.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentDataBase);

SegmentDataBase::SegmentDataBase() : mBaseDocId(INVALID_DOCID), mSegmentId(INVALID_SEGMENTID) {}

SegmentDataBase::SegmentDataBase(const SegmentDataBase& other) { *this = other; }

SegmentDataBase::~SegmentDataBase() {}

SegmentDataBase& SegmentDataBase::operator=(const SegmentDataBase& other)
{
    if (this == &other) {
        return *this;
    }
    mBaseDocId = other.mBaseDocId;
    mSegmentId = other.mSegmentId;
    mSegmentDirName = other.mSegmentDirName;
    mLifecycle = other.mLifecycle;
    return *this;
}
}} // namespace indexlib::index_base
