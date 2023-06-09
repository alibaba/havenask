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
#ifndef __INDEXLIB_BUILDING_SEGMENT_DATA_H
#define __INDEXLIB_BUILDING_SEGMENT_DATA_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class BuildingSegmentData : public SegmentData
{
public:
    BuildingSegmentData();
    BuildingSegmentData(const config::BuildConfig& buildConfig);
    BuildingSegmentData(const BuildingSegmentData& segData);
    ~BuildingSegmentData();

public:
    void Init(const file_system::DirectoryPtr& directory, bool isSubSegment = false);

    void PrepareDirectory();

    BuildingSegmentData CreateShardingSegmentData(uint32_t idx);

    bool IsBuildingSegment() const override { return true; }

private:
    bool mIsSubSegment;
    file_system::DirectoryPtr mPartitionDir;
    config::BuildConfig mBuildConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSegmentData);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_BUILDING_SEGMENT_DATA_H
