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

#include "autil/Log.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"

namespace indexlibv2::table {

class BucketMapOperation : public framework::IndexOperation
{
public:
    BucketMapOperation(const framework::IndexOperationDescription& description);
    ~BucketMapOperation() = default;

    Status Execute(const framework::IndexTaskContext& context) override;

public:
    static constexpr char OPERATION_TYPE[] = "bucket_map";

private:
    std::pair<Status, indexlib::index::BucketMaps>
    CreateBucketMaps(const framework::IndexTaskContext& context,
                     const index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos) const;
    std::pair<Status, index::IIndexMerger::SegmentMergeInfos>
    PrepareSegmentMergeInfos(const framework::IndexTaskContext& context) const;
    Status GetSegmentMergePlan(const framework::IndexTaskContext& context, SegmentMergePlan& segMergePlan) const;

private:
    Status RewriteMergeConfig(const indexlibv2::framework::IndexTaskContext& taskContext);
    const framework::IndexOperationDescription _description;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
