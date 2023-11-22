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

#include <algorithm>
#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class MultiPartSegmentDirectory : public SegmentDirectory
{
public:
    /*
     * @roots : the dirs to merge.
     * @versions in each dirs.
     */
    MultiPartSegmentDirectory(const std::vector<file_system::DirectoryPtr>& roots,
                              const std::vector<index_base::Version>& versions);
    virtual ~MultiPartSegmentDirectory();

public:
    file_system::DirectoryPtr GetRootDir() const override
    {
        // TODO:
        return GetFirstValidRootDir();
    }

    file_system::DirectoryPtr GetFirstValidRootDir() const override { return mRootDirs[0]; }

    std::string GetLatestCounterMapContent() const override;

    segmentid_t GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition, segmentid_t physicalSegId) const override;

    void GetPhysicalSegmentId(segmentid_t virtualSegId, segmentid_t& physicalSegId,
                              partitionid_t& partId) const override;

    SegmentDirectory* Clone() const override;

public:
    const std::vector<index_base::Version>& GetVersions() const { return mVersions; }
    const std::vector<file_system::DirectoryPtr>& GetRootDirs() const { return mRootDirs; }

    uint32_t GetPartitionCount() const;

    const std::vector<segmentid_t>& GetSegIdsByPartId(uint32_t partId) const;
    void Reload(const std::vector<index_base::Version>& versions);

protected:
    void InitOneSegment(partitionid_t partId, segmentid_t physicalSegId, segmentid_t virtualSegId,
                        const index_base::Version& version);
    void DoInit(bool hasSub, bool needDeletionMap) override;

protected:
    std::vector<file_system::DirectoryPtr> mRootDirs;
    std::vector<index_base::Version> mVersions;
    std::vector<std::vector<segmentid_t>> mPartSegIds; // virtual segIds

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartSegmentDirectory);

///////////////////////////////////////////////

inline uint32_t MultiPartSegmentDirectory::GetPartitionCount() const { return mPartSegIds.size(); }

inline const std::vector<segmentid_t>& MultiPartSegmentDirectory::GetSegIdsByPartId(uint32_t partId) const
{
    assert(partId < mPartSegIds.size());
    return mPartSegIds[partId];
}
}} // namespace indexlib::merger
