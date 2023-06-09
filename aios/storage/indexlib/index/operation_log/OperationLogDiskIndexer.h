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
#include "indexlib/base/Types.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/operation_log/OperationFieldInfo.h"
#include "indexlib/index/operation_log/OperationLogIndexer.h"

namespace indexlib::index {
class OperationLogConfig;

class OperationLogDiskIndexer : public indexlibv2::index::IDiskIndexer, public OperationLogIndexer
{
public:
    OperationLogDiskIndexer(segmentid_t segmentid);
    ~OperationLogDiskIndexer();

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;
    segmentid_t GetSegmentId() const override;
    bool GetOperationMeta(OperationMeta& operationMeta) const override;
    std::pair<Status, std::shared_ptr<SegmentOperationIterator>>
    CreateSegmentOperationIterator(size_t offset, const indexlibv2::framework::Locator& locator) override;
    std::pair<Status, std::shared_ptr<BatchOpLogIterator>> CreateBatchIterator() override;
    bool IsSealed() const override;

private:
    std::shared_ptr<OperationLogConfig> _opConfig;
    std::shared_ptr<file_system::Directory> _directory;
    std::shared_ptr<OperationMeta> _opMeta;
    std::shared_ptr<OperationFieldInfo> _operationFieldInfo;
    segmentid_t _segmentid;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
