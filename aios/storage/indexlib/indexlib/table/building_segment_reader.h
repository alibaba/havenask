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

#include <set>

#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace table {

class BuildingSegmentReader
{
public:
    BuildingSegmentReader(segmentid_t buildingSegId);
    virtual ~BuildingSegmentReader();

public:
    segmentid_t GetSegmentId() const { return _buildingSegId; }
    const std::set<segmentid_t>& GetDependentSegmentIds() const { return _dependentSegIds; }
    void AddDependentSegmentId(segmentid_t segId) { _dependentSegIds.insert(segId); }

protected:
    segmentid_t _buildingSegId;
    std::set<segmentid_t> _dependentSegIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSegmentReader);
}} // namespace indexlib::table
