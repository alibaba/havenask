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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/OpenOptions.h"

namespace autil {
class ThreadPool;
}
namespace indexlib::index {
template <typename DiskIndexerType, typename MemIndexerType>
class SingleAttributeBuilder;
class SingleInvertedIndexBuilder;
class SingleSummaryBuilder;
class ISinglePrimaryKeyBuilder;
class SingleDeletionMapBuilder;
class SingleOperationLogBuilder;
} // namespace indexlib::index
namespace indexlibv2::index {
class AttributeDiskIndexer;
class AttributeMemIndexer;
} // namespace indexlibv2::index
namespace indexlib::index::ann {
class SingleAithetaBuilder;
}
namespace indexlibv2::config {
class TabletOptions;
class ITabletSchema;
} // namespace indexlibv2::config
namespace indexlibv2::document {
class IDocumentBatch;
}
namespace indexlibv2::table {
class NormalMemSegment;
} // namespace indexlibv2::table

namespace indexlibv2::framework {
class TabletData;
}
namespace indexlib::util {
class GroupedThreadPool;
class WaitMemoryQuotaController;
} // namespace indexlib::util

namespace indexlib::table {
class SingleVirtualAttributeBuilder;

// ParallelBuilder does not support offline build mode currently. In offline build mode, update/delete is handled in
// operation log instead of in built segments. Check NormalTabletWriter::Open for more details.
class NormalTabletParallelBuilder : autil::NoCopyable
{
public:
    NormalTabletParallelBuilder();
    ~NormalTabletParallelBuilder();

public:
    Status Init(const std::shared_ptr<indexlibv2::table::NormalMemSegment>& normalBuildingSegment,
                const indexlibv2::config::TabletOptions* tabletOptions,
                const std::shared_ptr<autil::ThreadPool>& consistentModeBuildThreadPool,
                const std::shared_ptr<autil::ThreadPool>& inconsistentModeBuildThreadPool);
    Status SwitchBuildMode(indexlibv2::framework::OpenOptions::BuildMode buildMode);
    indexlibv2::framework::OpenOptions::BuildMode GetBuildMode() const;
    Status Build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch);

    Status PrepareForWrite(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                           const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData);

    void WaitFinish();

private:
    template <typename SingleBuilderType>
    Status InitSingleBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                              const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                              const std::string& indexTypeStr,
                              std::vector<std::unique_ptr<SingleBuilderType>>* builders);
    Status InitSingleInvertedIndexBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                           const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData);
    Status InitSingleAttributeBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                       const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData);
    Status InitSingleVirtualAttributeBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                              const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData);
    Status InitSinglePrimaryKeyBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                        const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData);
    Status
    InitSingleDeletionMapBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                  const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                                  std::vector<std::unique_ptr<indexlib::index::SingleDeletionMapBuilder>>* builders);
    template <typename BuilderType, typename BuildWorkItemType>
    void CreateBuildWorkItems(const std::vector<std::unique_ptr<BuilderType>>& builders,
                              const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch);
    Status ValidateConfigs(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);

public:
    indexlib::util::GroupedThreadPool* TEST_GetGroupedThreadPool() { return _buildThreadPool.get(); }

private:
    std::shared_ptr<indexlibv2::table::NormalMemSegment> _normalBuildingSegment;
    std::unique_ptr<indexlib::util::GroupedThreadPool> _buildThreadPool;
    std::unique_ptr<indexlib::util::WaitMemoryQuotaController> _docBatchMemController;

    bool _isOnline;
    indexlibv2::framework::OpenOptions::BuildMode _buildMode = indexlibv2::framework::OpenOptions::STREAM;
    std::shared_ptr<autil::ThreadPool> _consistentModeBuildThreadPool;
    std::shared_ptr<autil::ThreadPool> _inconsistentModeBuildThreadPool;

    bool _isPreparedForWrite = false;

    std::vector<std::unique_ptr<indexlib::index::SingleAttributeBuilder<indexlibv2::index::AttributeDiskIndexer,
                                                                        indexlibv2::index::AttributeMemIndexer>>>
        _singleAttributeBuilders;
    std::vector<std::unique_ptr<SingleVirtualAttributeBuilder>> _singleVirtualAttributeBuilders;
    std::vector<std::unique_ptr<indexlib::index::SingleInvertedIndexBuilder>> _singleInvertedIndexBuilders;
    std::vector<std::unique_ptr<indexlib::index::SingleSummaryBuilder>> _singleSummaryBuilders;
    std::vector<std::unique_ptr<indexlib::index::ann::SingleAithetaBuilder>> _singleAnnBuilders;
    std::vector<std::unique_ptr<indexlib::index::SingleOperationLogBuilder>> _singleOpLogBuilders;
    std::vector<std::unique_ptr<indexlib::index::ISinglePrimaryKeyBuilder>> _singlePrimaryKeyBuilders;
    std::vector<std::unique_ptr<indexlib::index::SingleDeletionMapBuilder>> _singleDeletionMapBuilders;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
