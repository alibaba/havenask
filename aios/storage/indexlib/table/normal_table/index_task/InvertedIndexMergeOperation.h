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
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/table/index_task/merger/IndexMergeOperation.h"

namespace indexlib::table {
class InvertedIndexMergeOperation : public indexlibv2::table::IndexMergeOperation
{
public:
    explicit InvertedIndexMergeOperation(const indexlibv2::framework::IndexOperationDescription& opDesc);
    ~InvertedIndexMergeOperation() = default;

protected:
    Status Init(const std::shared_ptr<indexlibv2::config::ITabletSchema> schema) override;

    Status Execute(const indexlibv2::framework::IndexTaskContext& context) override;
    std::string GetDebugString() const override;

    Status GetAllParameters(const indexlibv2::framework::IndexTaskContext& context,
                            const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
                            std::map<std::string, std::any>& params) const;

private:
    Status RewriteMergeConfig(const indexlibv2::framework::IndexTaskContext& taskContext);
    static Status PrepareTruncateMetaDir(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                         std::shared_ptr<indexlib::file_system::IDirectory>* result);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
