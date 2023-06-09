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
#include "indexlib/index_base/index_meta/version.h"

#include <beeper/beeper.h>
#include <limits>

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/index_path_util.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/RegularExpression.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

using FSEC = indexlib::file_system::ErrorCode;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, Version);

const uint32_t Version::CURRENT_FORMAT_VERSION = 2; //old version format, do not change

const uint32_t Version::INVALID_VERSION_FORMAT_NUM = std::numeric_limits<uint32_t>::max();
static uint32_t DEFAULT_VERSION_FORMAT_NUM_FOR_TEST = Version::INVALID_VERSION_FORMAT_NUM;

Version::Version()
    : mVersionId(INVALID_VERSION)
    , mLastSegmentId(INVALID_SEGMENTID)
    , mTimestamp(INVALID_TIMESTAMP)
    , mSchemaVersionId(DEFAULT_SCHEMAID)
{
    SetFormatVersion(CURRENT_FORMAT_VERSION);
}

Version::Version(versionid_t versionId)
    : mVersionId(versionId)
    , mLastSegmentId(INVALID_SEGMENTID)
    , mTimestamp(INVALID_TIMESTAMP)
    , mSchemaVersionId(DEFAULT_SCHEMAID)
{
    SetFormatVersion(CURRENT_FORMAT_VERSION);
}

Version::Version(versionid_t versionId, int64_t timestamp)
    : mVersionId(versionId)
    , mLastSegmentId(INVALID_SEGMENTID)
    , mTimestamp(timestamp)
    , mSchemaVersionId(DEFAULT_SCHEMAID)
{
    SetFormatVersion(CURRENT_FORMAT_VERSION);
}

Version::Version(const Version& other)
    : mVersionId(other.mVersionId)
    , mLastSegmentId(other.mLastSegmentId)
    , mTimestamp(other.mTimestamp)
    , mLocator(other.mLocator)
    , mSegmentIds(other.mSegmentIds)
    , mLevelInfo(other.mLevelInfo)
    , mFormatVersion(other.mFormatVersion)
    , mSchemaVersionId(other.mSchemaVersionId)
    , mOngoingModifyOpIds(other.mOngoingModifyOpIds)
    , mDesc(other.mDesc)
    , mSegTemperatureMetas(other.mSegTemperatureMetas)
    , mSegStatisticVec(other.mSegStatisticVec)
    , mUpdateableSchemaStandards(other.mUpdateableSchemaStandards)
{
}

Version::~Version() {}

void Version::SetFormatVersion(uint32_t formatVersion)
{
    if (DEFAULT_VERSION_FORMAT_NUM_FOR_TEST != INVALID_VERSION_FORMAT_NUM) {
        formatVersion = DEFAULT_VERSION_FORMAT_NUM_FOR_TEST;
    }
    mFormatVersion = formatVersion;
}

void Version::Jsonize(JsonWrapper& json)
{
    json.Jsonize("versionid", mVersionId);
    json.Jsonize("segments", mSegmentIds);
    json.Jsonize("schema_version", mSchemaVersionId, mSchemaVersionId);
    json.Jsonize("description", mDesc, mDesc);
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("level_info", mLevelInfo);
        json.Jsonize("format_version", mFormatVersion);
        json.Jsonize("last_segmentid", mLastSegmentId);
        string locatorString = autil::StringUtil::strToHexStr(mLocator.ToString());
        json.Jsonize("locator", locatorString);
        int64_t ts = mTimestamp;
        json.Jsonize("timestamp", ts);

        // remove invalid opId which is not in current schema id
        vector<schema_opid_t> ongoingIds = mOngoingModifyOpIds;
        auto it = remove_if(ongoingIds.begin(), ongoingIds.end(),
                            [this](schema_opid_t id) { return id > (schema_opid_t)mSchemaVersionId; });
        ongoingIds.erase(it, ongoingIds.end());
        if (!ongoingIds.empty()) {
            json.Jsonize("ongoing_modify_operations", ongoingIds);
        }
        if (!mSegTemperatureMetas.empty()) {
            sort(mSegTemperatureMetas.begin(), mSegTemperatureMetas.end());
            json.Jsonize("segment_temperature_metas", mSegTemperatureMetas);
        }
        if (!mSegStatisticVec.empty()) {
            sort(mSegStatisticVec.begin(), mSegStatisticVec.end(),
                 indexlibv2::framework::SegmentStatistics::CompareBySegmentId);
            json.Jsonize(indexlibv2::framework::SegmentStatistics::JSON_KEY, mSegStatisticVec);
        }
        if (!mUpdateableSchemaStandards.IsEmpty()) {
            string jsonString = ToJsonString(mUpdateableSchemaStandards);
            json.Jsonize("updateable_schema_standards", jsonString);
        }
    } else {
        string locatorString;
        json.Jsonize("locator", locatorString, locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        mLocator.SetLocator(locatorString);

        int64_t ts = INVALID_TIMESTAMP;
        json.Jsonize("timestamp", ts, ts);
        mTimestamp = ts;

        autil::legacy::json::JsonMap jsonMap = json.GetMap();
        if (jsonMap.find("last_segmentid") == jsonMap.end()) {
            if (mSegmentIds.size() == 0) {
                mLastSegmentId = INVALID_SEGMENTID;
            } else {
                mLastSegmentId = mSegmentIds[mSegmentIds.size() - 1];
            }
        } else {
            json.Jsonize("last_segmentid", mLastSegmentId);
        }

        if (jsonMap.find("format_version") == jsonMap.end()) {
            mFormatVersion = 0;
        } else {
            json.Jsonize("format_version", mFormatVersion);
        }

        if (jsonMap.find("level_info") == jsonMap.end()) {
            mLevelInfo.AddLevel(indexlibv2::framework::topo_sequence);
            for (auto segId : mSegmentIds) {
                mLevelInfo.AddSegment(segId);
            }
        } else {
            json.Jsonize("level_info", mLevelInfo);
        }
        json.Jsonize("ongoing_modify_operations", mOngoingModifyOpIds, mOngoingModifyOpIds);
        json.Jsonize("segment_temperature_metas", mSegTemperatureMetas, mSegTemperatureMetas);
        json.Jsonize(indexlibv2::framework::SegmentStatistics::JSON_KEY, mSegStatisticVec, mSegStatisticVec);
        string updateableSchemaStandards;
        json.Jsonize("updateable_schema_standards", updateableSchemaStandards, updateableSchemaStandards);
        if (!updateableSchemaStandards.empty()) {
            FromJsonString(mUpdateableSchemaStandards, updateableSchemaStandards);
        }
    }

    Validate(mSegmentIds);
}

void Version::FromString(const string& str) { autil::legacy::FromJsonString(*this, str); }

string Version::ToString(bool compact) const { return autil::legacy::ToJsonString(*this, compact); }

bool Version::HasSegment(segmentid_t segment) const
{
    SegmentIdVec::const_iterator iter = find(mSegmentIds.begin(), mSegmentIds.end(), segment);
    return (iter != mSegmentIds.end());
}

bool Version::Load(const string& path)
{
    string content;
    auto ec = FslibWrapper::AtomicLoad(path, content).Code();
    if (ec == FSEC_NOENT) {
        return false;
    }
    THROW_IF_FS_ERROR(ec, "load version [%s] failed", path.c_str());
    FromString(content);
    return true;
}

bool Version::Load(const file_system::DirectoryPtr& directory, const string& fileName)
{
    string content;
    if (!directory->LoadMayNonExist(fileName, content, true)) {
        IE_LOG(WARN, "version file [%s] not exist in dir [%s]", fileName.c_str(), directory->DebugString().c_str());
        return false;
    }
    FromString(content);
    return true;
}

string Version::GetVersionFileName(versionid_t versionId)
{
    stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return ss.str();
}

void Version::AddSegTemperatureMeta(const SegmentTemperatureMeta& temperatureMeta)
{
    if (!HasSegment(temperatureMeta.segmentId)) {
        return;
    }
    auto iter = mSegTemperatureMetas.begin();
    while (iter != mSegTemperatureMetas.end()) {
        if (iter->segmentId == temperatureMeta.segmentId) {
            iter = mSegTemperatureMetas.erase(iter);
        } else {
            ++iter;
        }
    }
    mSegTemperatureMetas.push_back(temperatureMeta);
}

void Version::AddSegmentStatistics(const indexlibv2::framework::SegmentStatistics& segmentStatistics)
{
    auto segmentId = segmentStatistics.GetSegmentId();
    if (!mSegStatisticVec.empty()) {
        auto lastStatSegId = mSegStatisticVec.back().GetSegmentId();
        if (segmentId <= lastStatSegId) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "cur statistic segment id [%d] <= last segment id [%d]", segmentId,
                                 lastStatSegId);
        }
    }
    mSegStatisticVec.push_back(segmentStatistics);
}

bool Version::GetSegmentTemperatureMeta(segmentid_t segmentId, SegmentTemperatureMeta& temperatureMeta) const
{
    auto iter = mSegTemperatureMetas.begin();
    while (iter != mSegTemperatureMetas.end()) {
        if (iter->segmentId == segmentId) {
            temperatureMeta = *iter;
            return true;
        } else {
            ++iter;
        }
    }
    return false;
}

bool Version::GetSegmentStatistics(segmentid_t segmentId,
                                   indexlibv2::framework::SegmentStatistics* segmentStatistics) const
{
    auto it = std::lower_bound(mSegStatisticVec.begin(), mSegStatisticVec.end(), segmentId,
                               indexlibv2::framework::SegmentStatistics::CompareToSegmentId);
    if (it != mSegStatisticVec.end()) {
        if (it->GetSegmentId() == segmentId) {
            *segmentStatistics = *it;
            return true;
        }
    }
    return false;
}

void Version::RemoveSegmentStatistics(segmentid_t segmentId)
{
    auto iter = mSegStatisticVec.begin();
    while (iter != mSegStatisticVec.end()) {
        if (iter->GetSegmentId() == segmentId) {
            iter = mSegStatisticVec.erase(iter);
            break;
        } else {
            ++iter;
        }
    }
}

void Version::RemoveSegTemperatureMeta(segmentid_t segmentId)
{
    auto iter = mSegTemperatureMetas.begin();
    while (iter != mSegTemperatureMetas.end()) {
        if (iter->segmentId == segmentId) {
            iter = mSegTemperatureMetas.erase(iter);
        } else {
            ++iter;
        }
    }
}

void Version::Store(const file_system::DirectoryPtr& directory, bool overwrite)
{
    if (mVersionId == INVALID_VERSION) {
        INDEXLIB_FATAL_ERROR(InvalidVersion, "not support store invalid version!");
    }
    std::string versionFileName = GetVersionFileName();
    std::string versionFilePath = PathUtil::JoinPath(directory->GetOutputPath(), versionFileName);
    if (overwrite) {
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        directory->RemoveFile(versionFileName, /*mayNonExist=*/removeOption);
        auto ec =
            FslibWrapper::DeleteFile(versionFilePath, DeleteOption::Fence(directory->GetFenceContext().get(), true))
                .Code();
        THROW_IF_FS_ERROR(ec, "overwrite delete version file [%s] failed", versionFilePath.c_str());
    }

    const string& versionStr = ToString();

    IE_LOG(INFO, "store version to [%s]", versionFilePath.c_str());
    // TODO: online do the same thing
    IFileSystemPtr fs = directory->GetFileSystem();
    if (fs && fs->GetFileSystemOptions().isOffline) {
        assert(directory->GetLogicalPath().empty());
        std::vector<string> wishFileList, wishDirList;
        FillWishList(directory, &wishFileList, &wishDirList);
        auto ec = fs->CommitSelectedFilesAndDir(mVersionId, wishFileList, wishDirList, {}, versionFileName, versionStr,
                                                directory->GetFenceContext().get());
        THROW_IF_FS_ERROR(ec, "commit version file [%d] failed", mVersionId);
    } else {
        WriterOption writerOption;
        writerOption.atomicDump = true;
        // prohibitInMemDump is true here will trigger dir's Sync which will erase file node from
        // fileNodeCache PartitionSizeCalculatorTest.* will failed as a result In addition, store
        // version file (not in RT/JOIN index partition) should with writeOptions.prohibitInMemDump
        // = true writerOption.prohibitInMemDump = true;
        directory->Store(versionFileName, versionStr, writerOption);
    }

    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "store version [%d], segments [%s], ongoingModifyOps [%s], "
                                      "schemaId [%u], timestamp [%ld]",
                                      mVersionId, StringUtil::toString(mSegmentIds, ",").c_str(),
                                      StringUtil::toString(mOngoingModifyOpIds, ",").c_str(), mSchemaVersionId,
                                      mTimestamp);
}

void Version::FillWishList(const DirectoryPtr& rootDir, std::vector<string>* wishFileList,
                           std::vector<string>* wishDirList)
{
    wishFileList->emplace_back(INDEX_FORMAT_VERSION_FILE_NAME);
    wishFileList->emplace_back(GetSchemaFileName());
    wishFileList->emplace_back(INDEX_PARTITION_META_FILE_NAME);
    wishFileList->emplace_back(GetVersionFileName());
    wishFileList->emplace_back(string(DEPLOY_META_FILE_NAME_PREFIX) + "." + StringUtil::toString(GetVersionId()));
    wishFileList->emplace_back(ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE);
    wishDirList->emplace_back(TRUNCATE_META_DIR_NAME);
    wishDirList->emplace_back(ADAPTIVE_DICT_DIR_NAME);

    // Add segment dir
    Version::Iterator iter = CreateIterator();
    while (iter.HasNext()) {
        segmentid_t segmentId = iter.Next();
        string segmentDirName = GetSegmentDirName(segmentId);
        wishDirList->emplace_back(segmentDirName);
    }

    if (GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        return;
    }

    // Add patch related file and dir
    string patchMetaName = PartitionPatchMeta::GetPatchMetaFileName(GetVersionId());
    if (!rootDir->IsExist(patchMetaName)) {
        IE_LOG(INFO, "patch meta file not exist [%s]", patchMetaName.c_str());
        return;
    }
    PartitionPatchMeta patchMeta;
    patchMeta.Load(rootDir, GetVersionId());
    wishFileList->emplace_back(patchMetaName);
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext()) {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (!HasSegment(segMeta.GetSegmentId())) {
                continue;
            }
            string patchSegPath =
                util::PathUtil::JoinPath(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                                         GetSegmentDirName(segMeta.GetSegmentId()));

            IE_LOG(DEBUG, "fill wish list by patch index segment [%s]", patchSegPath.c_str());
            wishDirList->emplace_back(patchSegPath);
        }
    }
}

void Version::TEST_Store(const std::string& path, bool overwrite)
{
    if (mVersionId == INVALID_VERSION) {
        INDEXLIB_FATAL_ERROR(InvalidVersion, "not support store invalid version!");
    }
    const std::string versionFilePath = PathUtil::JoinPath(path, GetVersionFileName());
    const string& versionStr = ToString();
    IE_LOG(INFO, "store version to [%s]", versionFilePath.c_str());
    auto ec = FslibWrapper::AtomicStore(versionFilePath, versionStr, overwrite).Code();
    THROW_IF_FS_ERROR(ec, "write version file failed, file[%s], overwrite[%s]", versionFilePath.c_str(),
                      overwrite ? "true" : "false");
}

bool Version::IsValidSegmentDirName(const std::string& segDirName) const
{
    return IsValidSegmentDirName(segDirName, IsLegacySegmentDirName());
}

bool Version::IsValidSegmentDirName(const std::string& segDirName, bool isLegacy)
{
    string pattern;
    if (isLegacy) {
        pattern = string("^") + SEGMENT_FILE_NAME_PREFIX + "_[0-9]+$";
    } else {
        pattern = string("^") + SEGMENT_FILE_NAME_PREFIX + "_[0-9]+_level_[0-9]+$";
    }
    util::RegularExpression regex;
    bool ret = regex.Init(pattern);
    assert(ret);
    (void)ret;
    return regex.Match(segDirName);
}

Version operator-(const Version& left, const Version& right)
{
    Version result = left;
    for (auto segId : right.GetSegmentVector()) {
        result.RemoveSegment(segId);
    }
    return result;
}

void Version::AddSegment(segmentid_t segmentId)
{
    if (mLevelInfo.GetLevelCount() == 0) {
        mLevelInfo.AddLevel(indexlibv2::framework::topo_sequence);
    }
    SegmentTopologyInfo topoInfo;
    AddSegment(segmentId, topoInfo);
}

void Version::AddSegment(segmentid_t segmentId, const SegmentTopologyInfo& topoInfo)
{
    if (segmentId <= mLastSegmentId) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "cur segment id [%d] <= last segment id [%d]", segmentId, mLastSegmentId);
    }

    if (topoInfo.levelIdx >= mLevelInfo.GetLevelCount()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "cur level id [%u] >= levelCount [%lu]", topoInfo.levelIdx,
                             mLevelInfo.GetLevelCount());
    }

    indexlibv2::framework::LevelMeta& levelMeta = mLevelInfo.levelMetas[topoInfo.levelIdx];
    switch (topoInfo.levelTopology) {
    case indexlibv2::framework::topo_sequence:
        levelMeta.segments.push_back(segmentId);
        break;
    case indexlibv2::framework::topo_hash_mod:
        assert(topoInfo.columnIdx < levelMeta.segments.size());
        levelMeta.segments[topoInfo.columnIdx] = segmentId;
        break;
    case indexlibv2::framework::topo_hash_range:
    case indexlibv2::framework::topo_default:
        assert(false);
        return;
    }
    mSegmentIds.push_back(segmentId);
    mLastSegmentId = segmentId;
}

void Version::Validate(const SegmentIdVec& segmentIds)
{
    segmentid_t lastSegId = INVALID_SEGMENTID;
    for (size_t i = 0; i < segmentIds.size(); ++i) {
        segmentid_t curSegId = segmentIds[i];
        if (lastSegId != INVALID_SEGMENTID && lastSegId >= curSegId) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "last segment id [%d] >= cur segment id [%d]", lastSegId, curSegId);
        }
        lastSegId = curSegId;
    }
}

std::string Version::GetSegmentDirName(segmentid_t segId) const
{
    stringstream ss;
    if (IsLegacySegmentDirName()) {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId;
    } else {
        uint32_t levelIdx = 0;
        uint32_t inLevelIdx = 0;
        bool ret = mLevelInfo.FindPosition(segId, levelIdx, inLevelIdx);
        assert(ret);
        (void)ret;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_" << levelIdx;
    }
    return ss.str();
}

std::string Version::GetNewSegmentDirName(segmentid_t segId) const
{
    stringstream ss;
    if (IsLegacySegmentDirName()) {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId;
    } else {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
    }
    return ss.str();
}

segmentid_t Version::GetSegmentIdByDirName(const string& segDirName)
{
    string prefix = string(SEGMENT_FILE_NAME_PREFIX) + "_";
    if (segDirName.substr(0, prefix.size()) != prefix) {
        return INVALID_SEGMENTID;
    }
    string suffix = segDirName.substr(prefix.size());
    return StringUtil::fromString<segmentid_t>(suffix);
}

void Version::SyncSchemaVersionId(const IndexPartitionSchemaPtr& schema)
{
    if (schema) {
        mSchemaVersionId = schema->GetSchemaVersionId();
    }
}

string Version::GetSchemaFileName(schemavid_t schemaId) { return IndexPartitionSchema::GetSchemaFileName(schemaId); }

bool Version::ExtractSchemaIdBySchemaFile(const string& schemaFileName, schemavid_t& schemaId)
{
    if (schemaFileName == SCHEMA_FILE_NAME) {
        schemaId = DEFAULT_SCHEMAID;
        return true;
    }

    string prefixStr = SCHEMA_FILE_NAME + ".";
    if (schemaFileName.find(prefixStr) != 0) {
        return false;
    }
    string idStr = schemaFileName.substr(prefixStr.length());
    return StringUtil::fromString(idStr, schemaId);
}

void Version::AddDescription(const string& key, const string& value)
{
    auto iter = mDesc.find(key);
    if (iter == mDesc.end()) {
        mDesc.insert(make_pair(key, value));
        return;
    }
    iter->second = value;
}

bool Version::GetDescription(const string& key, string& value) const
{
    auto iter = mDesc.find(key);
    if (iter == mDesc.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

void Version::SetDescriptions(const map<string, string>& descs)
{
    for (auto iter = descs.begin(); iter != descs.end(); iter++) {
        mDesc[iter->first] = iter->second;
    }
}

void Version::MergeDescriptions(const Version& version)
{
    for (auto iter = version.mDesc.begin(); iter != version.mDesc.end(); iter++) {
        mDesc[iter->first] = iter->second;
    }
}

void Version::SetUpdateableSchemaStandards(const config::UpdateableSchemaStandards& standards)
{
    mUpdateableSchemaStandards = standards;
}

const config::UpdateableSchemaStandards& Version::GetUpdateableSchemaStandards() const
{
    return mUpdateableSchemaStandards;
}

void Version::SetDefaultVersionFormatNumForTest(uint32_t version) { DEFAULT_VERSION_FORMAT_NUM_FOR_TEST = version; }

indexlibv2::framework::VersionMeta Version::ToVersionMeta() const
{
    indexlibv2::framework::VersionMeta versionMeta;
    versionMeta._versionId = GetVersionId();
    versionMeta._segments = GetSegmentVector();
    versionMeta._timestamp = GetTimestamp();
    versionMeta._sealed = false;
    return versionMeta;
}

}} // namespace indexlib::index_base
