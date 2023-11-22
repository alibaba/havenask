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
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class JoinSegmentDirectory : public SegmentDirectory
{
    static const segmentid_t JOIN_SEGMENT_ID_MASK = (segmentid_t)0x1 << 29;

public:
    JoinSegmentDirectory();
    ~JoinSegmentDirectory();

public:
    SegmentDirectory* Clone() override { return new JoinSegmentDirectory(*this); }

    void CommitVersion(const config::IndexPartitionOptions& options) override;

    void AddSegment(segmentid_t segId, timestamp_t ts) override;

    segmentid_t FormatSegmentId(segmentid_t segId) const override { return ConvertToJoinSegmentId(segId); }

    bool IsMatchSegmentId(segmentid_t segId) const override { return IsJoinSegmentId(segId); }

    static segmentid_t ConvertToJoinSegmentId(segmentid_t segId) { return segId | JOIN_SEGMENT_ID_MASK; }

    static bool IsJoinSegmentId(segmentid_t segId) { return (segId & JOIN_SEGMENT_ID_MASK) > 0; }

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinSegmentDirectory);
}} // namespace indexlib::index_base
