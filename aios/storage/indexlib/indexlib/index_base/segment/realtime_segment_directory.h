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
#ifndef __INDEXLIB_REALTIME_SEGMENT_DIRECTORY_H
#define __INDEXLIB_REALTIME_SEGMENT_DIRECTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class RealtimeSegmentDirectory : public SegmentDirectory
{
    static const segmentid_t RT_SEGMENT_ID_MASK = (segmentid_t)0x1 << 30;

public:
    RealtimeSegmentDirectory(bool enableRecover = false);
    ~RealtimeSegmentDirectory();

public:
    SegmentDirectory* Clone() override { return new RealtimeSegmentDirectory(*this); }

    void CommitVersion(const config::IndexPartitionOptions& options) override;

    segmentid_t FormatSegmentId(segmentid_t segId) const override { return ConvertToRtSegmentId(segId); }

    bool IsMatchSegmentId(segmentid_t segId) const override { return IsRtSegmentId(segId); }

    static segmentid_t ConvertToRtSegmentId(segmentid_t segId) { return segId | RT_SEGMENT_ID_MASK; }

    static bool IsRtSegmentId(segmentid_t segId) { return (segId & RT_SEGMENT_ID_MASK) > 0; }

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

private:
    void RemoveVersionFiles(const file_system::DirectoryPtr& directory);

private:
    bool mEnableRecover;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimeSegmentDirectory);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_REALTIME_SEGMENT_DIRECTORY_H
