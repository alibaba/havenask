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

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
namespace indexlib { namespace partition {

class InMemorySegmentContainer
{
public:
    InMemorySegmentContainer();
    ~InMemorySegmentContainer();

public:
    void PushBack(const index_base::InMemorySegmentPtr& inMemSeg);
    void EvictOldestInMemSegment();
    index_base::InMemorySegmentPtr GetOldestInMemSegment() const;
    void WaitEmpty() const;

    size_t GetTotalMemoryUse() const;

    size_t Size() const
    {
        autil::ScopedLock lock(mInMemSegVecLock);
        return mInMemSegVec.size();
    }

private:
    std::vector<index_base::InMemorySegmentPtr> mInMemSegVec;
    mutable autil::ThreadCond mInMemSegVecLock;
    mutable volatile size_t mTotalMemUseCache;

    static const size_t INVALID_MEMORY_USE;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentContainer);
}} // namespace indexlib::partition
