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
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace merger {

class DumpStrategy
{
public:
    DumpStrategy(const SegmentDirectoryPtr& segDir) : mDumppingVersion(segDir->GetVersion()), mIsDirty(false)
    {
        mRootDirectory = segDir->GetRootDir();
    }

    DumpStrategy(const file_system::DirectoryPtr& rootDirectory, const index_base::Version& version)
        : mRootDirectory(rootDirectory)
        , mDumppingVersion(version)
        , mIsDirty(false)
    {
    }

    ~DumpStrategy() {}

public:
    segmentid_t GetLastSegmentId() const { return mDumppingVersion.GetLastSegment(); }
    void RemoveSegment(segmentid_t segId)
    {
        mDumppingVersion.RemoveSegment(segId);
        mIsDirty = true;
    }
    void AddMergedSegment(segmentid_t segId)
    {
        mDumppingVersion.AddSegment(segId);
        mIsDirty = true;
    }
    void AddMergedSegment(segmentid_t segId, const index_base::SegmentTopologyInfo& topoInfo)
    {
        mDumppingVersion.AddSegment(segId, topoInfo);
        mIsDirty = true;
    }
    const index_base::Version& GetVersion() const { return mDumppingVersion; }
    void IncreaseLevelCursor(uint32_t levelIdx)
    {
        indexlibv2::framework::LevelInfo& levelInfo = mDumppingVersion.GetLevelInfo();
        mIsDirty = true;
        if (levelIdx > 0 && levelIdx < levelInfo.GetLevelCount()) {
            indexlibv2::framework::LevelMeta& levelMeta = levelInfo.levelMetas[levelIdx];
            levelMeta.cursor = (levelMeta.cursor + 1) % levelMeta.segments.size();
        }
    }
    void IncVersionId() { mDumppingVersion.IncVersionId(); }
    std::string GetRootPath() const { return mRootDirectory->GetOutputPath(); }
    const file_system::DirectoryPtr GetRootDirectory() const { return mRootDirectory; }
    void Reload(int64_t ts, const document::Locator& locator)
    {
        index_base::VersionLoader::GetVersion(mRootDirectory, mDumppingVersion, INVALID_VERSIONID);
        mDumppingVersion.SetTimestamp(ts);
        mDumppingVersion.SetLocator(locator);
    }

    bool IsDirty() const { return mIsDirty; }

    void AddSegmentTemperatureMeta(const index_base::SegmentTemperatureMeta& temperatureMeta)
    {
        mDumppingVersion.AddSegTemperatureMeta(temperatureMeta);
    }

    void AddSegmentStatistics(const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStatistics)
    {
        if (segmentStatistics != nullptr) {
            mDumppingVersion.AddSegmentStatistics(*segmentStatistics);
        }
    }

    void RemoveSegmentTemperatureMeta(segmentid_t segmentId) { mDumppingVersion.RemoveSegTemperatureMeta(segmentId); }
    // just for test
    index_base::Version getDumpingVersion() { return mDumppingVersion; }
    void RevertVersion(const index_base::Version& version) { mDumppingVersion = version; }

private:
    file_system::DirectoryPtr mRootDirectory;
    index_base::Version mDumppingVersion;
    bool mIsDirty;
};

DEFINE_SHARED_PTR(DumpStrategy);
}} // namespace indexlib::merger
