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
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(config, KKVIndexConfig);

namespace indexlib { namespace partition {

class CompressRatioCalculator
{
public:
    CompressRatioCalculator();
    ~CompressRatioCalculator();

public:
    void Init(const index_base::PartitionDataPtr& partData, const config::IndexPartitionSchemaPtr& schema);

    double GetCompressRatio(const std::string& fileName) const;

private:
    void InitKVCompressRatio(const config::KVIndexConfigPtr& kvConfig, const index_base::PartitionDataPtr& partData);

    void InitKKVCompressRatio(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partData);

    void GetSegmentDatas(const index_base::PartitionDataPtr& partData, index_base::SegmentDataVector& segDataVec);

    double GetRatio(const index_base::SegmentDataVector& segDataVec, const std::string& filePath);

private:
    // first: FileName, second: ratio
    typedef std::map<std::string, double> RatioMap;
    RatioMap mRatioMap;

private:
    static constexpr double DEFAULT_COMPRESS_RATIO = 1.0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressRatioCalculator);
}} // namespace indexlib::partition
