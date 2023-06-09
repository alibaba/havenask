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
#ifndef __INDEXLIB_SEGMENT_DATA_CREATOR_H
#define __INDEXLIB_SEGMENT_DATA_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(config, BuildConfig);

namespace indexlib { namespace partition {

class SegmentDataCreator
{
public:
    SegmentDataCreator();
    ~SegmentDataCreator();

public:
    static index_base::BuildingSegmentData
    CreateNewSegmentData(const index_base::SegmentDirectoryPtr& segDir,
                         const index_base::InMemorySegmentPtr& lastDumpingSegment,
                         const config::BuildConfig& buildConfig);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDataCreator);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SEGMENT_DATA_CREATOR_H
