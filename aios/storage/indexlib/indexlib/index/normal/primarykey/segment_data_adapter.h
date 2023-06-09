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

#include "indexlib/index/primary_key/SegmentDataAdapter.h"
#include "indexlib/index_base/partition_data.h"

namespace indexlib::index {
class SegmentDataAdapter
{
public:
    static void Transform(const indexlib::index_base::PartitionDataPtr& partitionData,
                          std::vector<indexlibv2::index::SegmentDataAdapter::SegmentDataType>& segmentDatas);
};
} // namespace indexlib::index
