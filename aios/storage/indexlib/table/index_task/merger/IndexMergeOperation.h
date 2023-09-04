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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"

namespace indexlibv2::table {

class IndexMergeOperation : public framework::IndexOperation
{
public:
    static constexpr char OPERATION_TYPE[] = "index_merge_operation";

public:
    IndexMergeOperation(const framework::IndexOperationDescription& opDesc)
        : framework::IndexOperation(opDesc.GetId(), opDesc.UseOpFenceDir())
        , _desc(opDesc)
    {
    }
    virtual ~IndexMergeOperation() = default;

    virtual Status Init(const std::shared_ptr<config::ITabletSchema> schema);
    Status Execute(const framework::IndexTaskContext& context) override;
    virtual std::string GetDebugString() const override;

protected:
    virtual std::pair<Status, std::shared_ptr<config::IIndexConfig>>
    GetIndexConfig(const framework::IndexOperationDescription& opDesc, const std::string& indexType,
                   const std::string& indexName, const std::shared_ptr<config::ITabletSchema>& schema);
    virtual std::pair<Status, index::IIndexMerger::SegmentMergeInfos>
    PrepareSegmentMergeInfos(const framework::IndexTaskContext& context, bool prepareTaretSegDir);
    Status StoreMergedSegmentMetricsLegacy(const index::IIndexMerger::SegmentMergeInfos& segMergeInfos);
    Status StoreMergedSegmentMetrics(const index::IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                     const framework::IndexTaskContext& context);
    std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
    PrepareTargetSegDir(const std::shared_ptr<indexlib::file_system::IDirectory>& fenceDir, const std::string& segDir,
                        const std::vector<std::string>& mergeIndexDirs);
    Status GetSegmentMergePlan(const framework::IndexTaskContext& context, SegmentMergePlan& segMergePlan,
                               framework::Version& targetVersion);

protected:
    framework::IndexOperationDescription _desc;
    std::shared_ptr<config::IIndexConfig> _indexConfig;
    std::shared_ptr<index::IIndexMerger> _indexMerger;
    std::string _indexPath;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
