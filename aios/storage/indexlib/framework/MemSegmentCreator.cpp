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
#include "indexlib/framework/MemSegmentCreator.h"

#include <assert.h>
#include <type_traits>
#include <vector>

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, MemSegmentCreator);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

MemSegmentCreator::MemSegmentCreator(const std::string& tabletName, config::TabletOptions* tabletOptions,
                                     ITabletFactory* tabletFactory, IdGenerator* idGenerator,
                                     const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir)
    : _tabletName(tabletName)
    , _tabletOptions(tabletOptions)
    , _tabletFactory(tabletFactory)
    , _idGenerator(idGenerator)
    , _rootDir(rootDir)
{
}

MemSegmentCreator::~MemSegmentCreator() {}

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
MemSegmentCreator::PrepareSegmentDir(const std::string& segDir)
{
    auto physicalDir =
        indexlib::file_system::Directory::GetPhysicalDirectory(_rootDir->GetPhysicalPath(""))->GetIDirectory();
    auto status = _rootDir->RemoveDirectory(segDir, indexlib::file_system::RemoveOption::MayNonExist()).Status();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "rm exist segment dir failed");
        return std::make_pair(status, nullptr);
    }
    status = physicalDir->RemoveDirectory(segDir, indexlib::file_system::RemoveOption::MayNonExist()).Status();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "rm exist segment dir failed");
        return std::make_pair(status, nullptr);
    }

    indexlib::file_system::DirectoryOption options = indexlib::file_system::DirectoryOption::Mem();
    if (_tabletOptions->GetBuildConfig().GetIsEnablePackageFile()) {
        options = indexlib::file_system::DirectoryOption::PackageMem();
    }
    return _rootDir->MakeDirectory(segDir, options).StatusWith();
}

Status MemSegmentCreator::CreateSegmentMeta(segmentid_t newSegId, const std::shared_ptr<TabletData>& tabletData,
                                            const std::shared_ptr<config::ITabletSchema>& writeSchema,
                                            SegmentMeta* segMeta)
{
    assert(segMeta != nullptr);
    std::string segDirName = PathUtil::NewSegmentDirName(newSegId, 0);
    auto [status, segDir] = PrepareSegmentDir(segDirName);
    RETURN_IF_STATUS_ERROR(status, "prepare segment dir failed");
    segMeta->segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(segDir);
    segMeta->segmentMetrics = std::make_shared<indexlib::framework::SegmentMetrics>();
    segMeta->segmentId = newSegId;
    segMeta->schema = writeSchema;

    std::shared_ptr<framework::Segment> lastSegment;
    auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_UNSPECIFY);
    if (slice.begin() != slice.end()) {
        lastSegment = *slice.rbegin();
    }
    if (lastSegment == nullptr || Segment::IsMergedSegmentId(lastSegment->GetSegmentId())) {
        // make first segMeta from Version
        const auto& version = tabletData->GetOnDiskVersion();
        segMeta->segmentInfo->SetLocator(version.GetLocator());
        segMeta->segmentInfo->SetTimestamp(version.GetTimestamp());
        auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
        if (levelInfo != nullptr) {
            segMeta->segmentInfo->SetShardCount(levelInfo->GetShardCount());
        } else {
            AUTIL_LOG(INFO, "get shard count from on disk version failed, set it to[%u]",
                      framework::SegmentInfo::INVALID_SHARDING_COUNT);
            segMeta->segmentInfo->SetShardCount(framework::SegmentInfo::INVALID_SHARDING_COUNT);
        }
    } else {
        const std::shared_ptr<SegmentInfo>& segInfo = lastSegment->GetSegmentInfo();
        segMeta->segmentInfo->SetLocator(segInfo->GetLocator());
        segMeta->segmentInfo->SetTimestamp(segInfo->GetTimestamp());
        segMeta->segmentInfo->SetShardCount(segInfo->GetShardCount());
    }
    return Status::OK();
}

std::pair<Status, std::shared_ptr<MemSegment>>
MemSegmentCreator::CreateMemSegment(const BuildResource& buildResource, const std::shared_ptr<TabletData>& tabletData,
                                    const std::shared_ptr<config::ITabletSchema>& writeSchema)
{
    if (tabletData == nullptr) {
        TABLET_LOG(ERROR, "tablet data is null!");
        return std::make_pair(Status::Corruption(), nullptr);
    }

    SegmentMeta segmentMeta;
    segmentid_t newSegId = _idGenerator->GetNextSegmentId();
    if (!CreateSegmentMeta(newSegId, tabletData, writeSchema, &segmentMeta).IsOK()) {
        TABLET_LOG(ERROR, "CreateSegmentMeta failed!");
        return std::make_pair(Status::Corruption(), nullptr);
    }

    auto slice = tabletData->CreateSlice();
    indexlib::framework::SegmentMetrics* lastSegMetrics = nullptr;
    if (slice.begin() != slice.end()) {
        auto& lastSeg = *slice.rbegin();
        lastSegMetrics = lastSeg->GetSegmentMetrics().get();
    }
    auto memSegment = _tabletFactory->CreateMemSegment(segmentMeta);
    if (memSegment == nullptr) {
        TABLET_LOG(ERROR, "create MemSegment failed!");
        return std::make_pair(Status::InternalError("create MemSegment failed!"), nullptr);
    }
    auto status = memSegment->Open(buildResource, lastSegMetrics);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "open MemSegment failed!");
        return std::make_pair(status, nullptr);
    }
    _idGenerator->CommitNextSegmentId();
    return std::make_pair(Status::OK(), std::shared_ptr<MemSegment>(memSegment.release()));
}

} // namespace indexlibv2::framework
