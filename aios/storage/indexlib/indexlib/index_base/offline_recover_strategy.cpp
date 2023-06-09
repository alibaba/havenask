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
#include "indexlib/index_base/offline_recover_strategy.h"

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;

using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
namespace indexlib { namespace index_base {
using namespace indexlib::util;

IE_LOG_SETUP(index_base, OfflineRecoverStrategy);

OfflineRecoverStrategy::OfflineRecoverStrategy() {}

OfflineRecoverStrategy::~OfflineRecoverStrategy() {}

Version OfflineRecoverStrategy::Recover(Version version, const DirectoryPtr& physicalDir,
                                        RecoverStrategyType recoverType)
{
    IE_LOG(INFO, "Begin recover partition!");
    segmentid_t lastSegmentId = version.GetLastSegment();
    vector<SegmentInfoPair> lostSegments;
    GetLostSegments(physicalDir, version, lastSegmentId, lostSegments);

    IE_LOG(INFO, "Latest ondisk version [%s]", version.ToString(true).c_str());
    Version recoveredVersion = version;

    if (recoverType == RecoverStrategyType::RST_VERSION_LEVEL) {
        CleanUselessSegments(lostSegments, 0, physicalDir);
        IE_LOG(INFO, "Recovered version [%s]", recoveredVersion.ToString(true).c_str());
        IE_LOG(INFO, "End recover partition!");
        return recoveredVersion;
    }
    for (size_t i = 0; i < lostSegments.size(); i++) {
        segmentid_t segId = lostSegments[i].first;
        string segDirName = lostSegments[i].second;
        DirectoryPtr segmentDirectory = physicalDir->GetDirectory(segDirName, false);
        if (!segmentDirectory) {
            continue;
        }

        SegmentInfo segInfo;
        if (!segInfo.Load(segmentDirectory) || segInfo.mergedSegment) {
            CleanUselessSegments(lostSegments, i, physicalDir);
            break;
        }
        recoveredVersion.AddSegment(segId);
        recoveredVersion.SetTimestamp(segInfo.timestamp);
        document::Locator locator(segInfo.GetLocator().Serialize());
        recoveredVersion.SetLocator(locator);
        try {
            ReadSegmentTemperatureMeta(segInfo, segId, recoveredVersion);
        } catch (...) {
            IE_LOG(ERROR, "recover segment [%d] read segment temperature failed", segId);
        }
    }

    IE_LOG(INFO, "Recovered version [%s]", recoveredVersion.ToString(true).c_str());
    IE_LOG(INFO, "End recover partition!");
    return recoveredVersion;
}

void OfflineRecoverStrategy::ReadSegmentTemperatureMeta(const SegmentInfo& segmentInfo, segmentid_t segmentId,
                                                        Version& version)
{
    string value;
    if (segmentInfo.GetDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, value)) {
        json::JsonMap temperatureMetrics;
        try {
            FromJsonString(temperatureMetrics, value);
        } catch (...) {
            IE_LOG(ERROR, "jsonize metric [%s] failed", value.c_str());
            return;
        }
        SegmentTemperatureMeta meta;
        if (SegmentTemperatureMeta::InitFromJsonMap(temperatureMetrics, meta)) {
            meta.segmentId = segmentId;
            version.AddSegTemperatureMeta(meta);
        }
    }
}

void OfflineRecoverStrategy::RemoveUselessSegments(Version version, const DirectoryPtr& physicalDir)
{
    IE_LOG(INFO, "Begin remove [%s] useless segments not in version [%d]!", physicalDir->DebugString().c_str(),
           version.GetVersionId());
    vector<SegmentInfoPair> uselessSegments;
    FileList fileList;
    physicalDir->ListDir("", fileList);

    for (size_t i = 0; i < fileList.size(); i++) {
        if (!version.IsValidSegmentDirName(fileList[i])) {
            continue;
        }
        segmentid_t segId = Version::GetSegmentIdByDirName(fileList[i]);
        if (!version.HasSegment(segId)) {
            uselessSegments.push_back({segId, fileList[i]});
        }
    }
    sort(uselessSegments.begin(), uselessSegments.end());
    CleanUselessSegments(uselessSegments, 0, physicalDir);
}

void OfflineRecoverStrategy::RemoveLostSegments(Version version, const DirectoryPtr& physicalDir)
{
    IE_LOG(INFO, "Begin remove lost segments in [%s] for version [%d]!", physicalDir->DebugString().c_str(),
           version.GetVersionId());
    vector<SegmentInfoPair> lostSegments;
    GetLostSegments(physicalDir, version, version.GetLastSegment(), lostSegments);
    CleanUselessSegments(lostSegments, 0, physicalDir);
}

void OfflineRecoverStrategy::GetLostSegments(const DirectoryPtr& directory, const Version& version,
                                             segmentid_t lastSegmentId, vector<SegmentInfoPair>& lostSegments)
{
    FileList fileList;
    // std::string physicalRoot = directory->GetOutputPath();
    // auto ec = FslibWrapper::ListDir(physicalRoot, fileList).Code();
    // if (ec != FSEC_OK && ec != FSEC_NOENT) {
    //     THROW_IF_FS_ERROR(ec, "List dir [%s] failed", physicalRoot.c_str());
    // }
    directory->ListDir("", fileList);
    for (size_t i = 0; i < fileList.size(); i++) {
        if (!version.IsValidSegmentDirName(fileList[i])) {
            continue;
        }

        segmentid_t segId = Version::GetSegmentIdByDirName(fileList[i]);
        if (segId > lastSegmentId) {
            lostSegments.push_back({segId, fileList[i]});
            // if (directory->GetFileSystem()) {
            //     directory->GetFileSystem()->MountDir(physicalRoot, fileList[i], fileList[i], FSMT_READ_WRITE, true);
            // }
        }
    }
    sort(lostSegments.begin(), lostSegments.end());
}

void OfflineRecoverStrategy::CleanUselessSegments(const vector<SegmentInfoPair>& lostSegments,
                                                  size_t firstUselessSegIdx, const DirectoryPtr& rootDirectory)
{
    for (size_t i = firstUselessSegIdx; i < lostSegments.size(); i++) {
        IE_LOG(INFO, "remove lost segment [%s]", lostSegments[i].second.c_str());
        rootDirectory->RemoveDirectory(lostSegments[i].second);
    }
}
}} // namespace indexlib::index_base
