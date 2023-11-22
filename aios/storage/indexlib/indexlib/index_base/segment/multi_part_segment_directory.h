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

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

// only support read
class MultiPartSegmentDirectory : public SegmentDirectory
{
public:
    MultiPartSegmentDirectory();
    MultiPartSegmentDirectory(const MultiPartSegmentDirectory& other);
    ~MultiPartSegmentDirectory();

public:
    void Init(const std::vector<file_system::DirectoryPtr>& directoryVec, bool hasSub = false);

    void Init(const std::vector<file_system::DirectoryPtr>& directoryVec,
              const std::vector<index_base::Version>& versions, bool hasSub = false);

    virtual SegmentDirectory* Clone();
    virtual file_system::DirectoryPtr GetSegmentParentDirectory(segmentid_t segId) const;
    virtual file_system::DirectoryPtr GetSegmentFsDirectory(segmentid_t segId) const;

    virtual void SetSubSegmentDir();
    virtual const index_base::IndexFormatVersion& GetIndexFormatVersion() const;

    const std::vector<SegmentDirectoryPtr>& GetSegmentDirectories() const { return mSegmentDirectoryVec; }

    segmentid_t EncodeToVirtualSegmentId(size_t partitionIdx, segmentid_t phySegId) const;

    bool DecodeVirtualSegmentId(segmentid_t virtualSegId, segmentid_t& physicalSegId, size_t& partitionId) const;

private:
    SegmentDirectoryPtr GetSegmentDirectory(segmentid_t segId) const;

private:
    typedef std::vector<segmentid_t> SegmentIdVector;
    std::vector<SegmentDirectoryPtr> mSegmentDirectoryVec;
    // [begin, end)
    SegmentIdVector mEndSegmentIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartSegmentDirectory);
}} // namespace indexlib::index_base
