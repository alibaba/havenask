#include <autil/StringUtil.h>
#include <beeper/beeper.h>
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/misc/regular_expression.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, Version);

const uint32_t Version::CURRENT_FORMAT_VERSION = 2;

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
{}

Version::~Version() 
{
}

void Version::SetFormatVersion(uint32_t formatVersion)
{
    formatVersion = EnvUtil::GetEnv("VERSION_FORMAT_NUM", formatVersion);
    mFormatVersion = formatVersion;
}

void Version::Jsonize(JsonWrapper& json)
{
    json.Jsonize("versionid", mVersionId);
    json.Jsonize("segments", mSegmentIds);
    json.Jsonize("schema_version", mSchemaVersionId, mSchemaVersionId);
    if (json.GetMode() == TO_JSON)
    {
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
                            [this](schema_opid_t id)
                            { return id > (schema_opid_t)mSchemaVersionId; });
        ongoingIds.erase(it, ongoingIds.end());
        if (!ongoingIds.empty())
        {
            json.Jsonize("ongoing_modify_operations", ongoingIds);
        }
    }
    else
    {
        string locatorString;
        json.Jsonize("locator", locatorString, locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        mLocator.SetLocator(locatorString);
        
        int64_t ts = INVALID_TIMESTAMP;
        json.Jsonize("timestamp", ts, ts);
        mTimestamp = ts;

        autil::legacy::json::JsonMap jsonMap = json.GetMap();
        if (jsonMap.find("last_segmentid") == jsonMap.end())
        {
            if (mSegmentIds.size() == 0)
            {
                mLastSegmentId = INVALID_SEGMENTID;
            }
            else
            {
                mLastSegmentId = mSegmentIds[mSegmentIds.size() - 1];
            }
        }
        else
        {
            json.Jsonize("last_segmentid", mLastSegmentId);
        }

        if (jsonMap.find("format_version") == jsonMap.end())
        {
            mFormatVersion = 0;
        }
        else
        {
            json.Jsonize("format_version", mFormatVersion);
        }

        if (jsonMap.find("level_info") == jsonMap.end())
        {
            mLevelInfo.AddLevel(topo_sequence);
            for (auto segId : mSegmentIds)
            {
                mLevelInfo.AddSegment(segId);
            }
        }
        else
        {
            json.Jsonize("level_info", mLevelInfo);
        }
        json.Jsonize("ongoing_modify_operations",
                     mOngoingModifyOpIds, mOngoingModifyOpIds);
    }

    Validate(mSegmentIds);
}

void Version::FromString(const string& str)
{
    autil::legacy::FromJsonString(*this, str);
}

string Version::ToString() const
{
    return autil::legacy::ToJsonString(*this);
}

bool Version::HasSegment(segmentid_t segment) const
{
    SegmentIdVec::const_iterator iter = 
        find(mSegmentIds.begin(), mSegmentIds.end(), segment);
    return (iter != mSegmentIds.end());
}

bool Version::Load(const string& path)
{
    string content;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(path, content, true))
        {
            return false;
        }
    }
    catch (...)
    {
        throw;
    }
    
    FromString(content);
    return true;
}

bool Version::Load(const file_system::DirectoryPtr& directory,
                   const string& fileName)
{
    string content;
    if (!directory->LoadMayNonExist(fileName, content, true))
    {
        IE_LOG(ERROR, "version file [%s] not exist in dir [%s]",
               fileName.c_str(), directory->GetPath().c_str());
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

void Version::Store(const string& dir, bool overwrite) const
{
    if (mVersionId == INVALID_VERSION)
    {
        INDEXLIB_FATAL_ERROR(InvalidVersion, "not support store invalid version!");
    }
    
    const string& versionPath = FileSystemWrapper::JoinPath(
            dir, GetVersionFileName());
    if (overwrite)
    {
        FileSystemWrapper::DeleteIfExist(versionPath);
    }

    const string& versionStr = ToString();
    IE_LOG(INFO, "store version to [%s]", versionPath.c_str());
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "store version [%u], segments [%s], ongoingModifyOps [%s], "
            "schemaId [%u], timestamp [%ld]",
            mVersionId, StringUtil::toString(mSegmentIds, ",").c_str(),
            StringUtil::toString(mOngoingModifyOpIds, ",").c_str(),
            mSchemaVersionId, mTimestamp);
    
    FileSystemWrapper::AtomicStore(versionPath, versionStr);
}

void Version::Store(const file_system::DirectoryPtr& directory, bool overwrite)
{
    if (mVersionId == INVALID_VERSION)
    {
        INDEXLIB_FATAL_ERROR(InvalidVersion, "not support store invalid version!");
    }

    const string& versionFile = GetVersionFileName();
    if (overwrite)
    {
        directory->RemoveFile(versionFile, true);
    }
    const string& versionStr = ToString();
    IE_LOG(INFO, "store version to [%s/%s]",
           directory->GetPath().c_str(), versionFile.c_str());
    directory->Store(versionFile, versionStr, true);

    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "store version [%d], segments [%s], ongoingModifyOps [%s], "
            "schemaId [%u], timestamp [%ld]",
            mVersionId, StringUtil::toString(mSegmentIds, ",").c_str(),
            StringUtil::toString(mOngoingModifyOpIds, ",").c_str(),
            mSchemaVersionId, mTimestamp);
}

bool Version::IsValidSegmentDirName(const std::string& segDirName) const
{
    return IsValidSegmentDirName(segDirName,
                                 IsLegacySegmentDirName());
}

bool Version::IsValidSegmentDirName(const std::string& segDirName, bool isLegacy)
{
    string pattern;
    if (isLegacy)
    {
        pattern = string("^") + SEGMENT_FILE_NAME_PREFIX + "_[0-9]+$";
    }
    else
    {
        pattern = string("^") + SEGMENT_FILE_NAME_PREFIX + "_[0-9]+_level_[0-9]+$";
    }
    misc::RegularExpression regex;
    bool ret = regex.Init(pattern);
    assert(ret);
    (void)ret;
    return regex.Match(segDirName);
}

Version operator - (const Version& left, const Version& right)
{
    Version result = left;
    for (auto segId : right.GetSegmentVector())
    {
        result.RemoveSegment(segId);
    }
    return result;
}

void Version::AddSegment(segmentid_t segmentId)
{
    if (mLevelInfo.GetLevelCount() == 0)
    {
        mLevelInfo.AddLevel(topo_sequence);
    }
    SegmentTopologyInfo topoInfo;
    AddSegment(segmentId, topoInfo);
}

void Version::AddSegment(segmentid_t segmentId, const SegmentTopologyInfo& topoInfo)
{
    if (segmentId <= mLastSegmentId)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "cur segment id [%d] <= last segment id [%d]", 
                             segmentId, mLastSegmentId);
    }

    if (topoInfo.levelIdx >= mLevelInfo.GetLevelCount())
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "cur level id [%u] >= levelCount [%lu]", 
                             topoInfo.levelIdx, mLevelInfo.GetLevelCount());
    }

    LevelMeta& levelMeta = mLevelInfo.levelMetas[topoInfo.levelIdx];
    switch(topoInfo.levelTopology)
    {
    case topo_sequence:
        levelMeta.segments.push_back(segmentId);
        break;
    case topo_hash_mod:
        assert(topoInfo.columnIdx < levelMeta.segments.size());
        levelMeta.segments[topoInfo.columnIdx] = segmentId;
        break;
    case topo_hash_range:
    case topo_default:
        assert(false);
        return;
    }
    mSegmentIds.push_back(segmentId);
    mLastSegmentId = segmentId;
}

void Version::Validate(const SegmentIdVec& segmentIds)
{
    segmentid_t lastSegId = INVALID_SEGMENTID;
    for (size_t i = 0; i < segmentIds.size(); ++i)
    {
        segmentid_t curSegId = segmentIds[i];
        if (lastSegId != INVALID_SEGMENTID && lastSegId >= curSegId)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "last segment id [%d] >= cur segment id [%d]", 
                    lastSegId, curSegId);
        }
        lastSegId = curSegId;
    }
}

std::string Version::GetSegmentDirName(segmentid_t segId) const
{
    stringstream ss;
    if (IsLegacySegmentDirName())
    {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId;
    }
    else
    {
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
    if (IsLegacySegmentDirName())
    {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId;
    }
    else
    {
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
    }
    return ss.str();
}

segmentid_t Version::GetSegmentIdByDirName(const string& segDirName)
{
    string prefix = string(SEGMENT_FILE_NAME_PREFIX) + "_";
    if (segDirName.substr(0, prefix.size()) != prefix)
    {
        return INVALID_SEGMENTID;
    }
    string suffix = segDirName.substr(prefix.size());
    return StringUtil::fromString<segmentid_t>(suffix);
}

void Version::SyncSchemaVersionId(const IndexPartitionSchemaPtr& schema)
{
    if (schema)
    {
        mSchemaVersionId = schema->GetSchemaVersionId();
    }
}

string Version::GetSchemaFileName(schemavid_t schemaId)
{
    if (schemaId == DEFAULT_SCHEMAID)
    {
        return index::SCHEMA_FILE_NAME;
    }
    return index::SCHEMA_FILE_NAME + "." + StringUtil::toString(schemaId);
}

bool Version::ExtractSchemaIdBySchemaFile(
        const string& schemaFileName, schemavid_t &schemaId)
{
    if (schemaFileName == index::SCHEMA_FILE_NAME)
    {
        schemaId = DEFAULT_SCHEMAID;
        return true;
    }

    string prefixStr = index::SCHEMA_FILE_NAME + ".";
    if (schemaFileName.find(prefixStr) != 0)
    {
        return false;
    }
    string idStr = schemaFileName.substr(prefixStr.length());
    return StringUtil::fromString(idStr, schemaId);
}


IE_NAMESPACE_END(index_base);

