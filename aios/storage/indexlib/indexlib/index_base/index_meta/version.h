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

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/updateable_schema_standards.h"
#include "indexlib/document/locator.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_STRUCT(index_base, SegmentTopologyInfo);

namespace indexlib { namespace index_base {

class Version : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<segmentid_t> SegmentIdVec;
    typedef std::vector<schema_opid_t> ModifyOpIdVec;
    typedef std::vector<SegmentTemperatureMeta> SegTemperatureVec;
    typedef std::vector<indexlibv2::framework::SegmentStatistics> SegmentStatisticVec;

    class Iterator
    {
    public:
        Iterator(const SegmentIdVec& vec) : mVec(vec), mIter(vec.begin()) {}
        Iterator(const Iterator& iter) : mVec(iter.mVec), mIter(iter.mVec.begin()) {}
        bool HasNext() const { return (mIter != mVec.end()); }
        segmentid_t Next() { return (*mIter++); }

    private:
        const SegmentIdVec& mVec;
        SegmentIdVec::const_iterator mIter;
    };

public:
    Version();
    explicit Version(versionid_t versionId);
    Version(versionid_t versionId, int64_t timestamp);
    Version(const Version& other);
    ~Version();

public:
    void Jsonize(JsonWrapper& json) override;

    void FromString(const std::string& str);
    std::string ToString(bool compact = false) const;

    indexlibv2::framework::VersionMeta ToVersionMeta() const;
    void Clear();

    void SetOngoingModifyOperations(const ModifyOpIdVec& opIds) { mOngoingModifyOpIds = opIds; }
    const ModifyOpIdVec& GetOngoingModifyOperations() const { return mOngoingModifyOpIds; }

    const SegTemperatureVec& GetSegTemperatureMetas() const { return mSegTemperatureMetas; }
    void AddSegTemperatureMeta(const SegmentTemperatureMeta& temperatureMeta);
    void AddSegmentStatistics(const indexlibv2::framework::SegmentStatistics& segmentStatistics);
    void RemoveSegTemperatureMeta(segmentid_t segmentId);
    void RemoveSegmentStatistics(segmentid_t segmentId);
    bool GetSegmentTemperatureMeta(segmentid_t segmentId, SegmentTemperatureMeta& temperatureMeta) const;

    const SegmentStatisticVec& GetSegmentStatisticsVector() const { return mSegStatisticVec; }
    bool GetSegmentStatistics(segmentid_t segmentId, indexlibv2::framework::SegmentStatistics* segmentStatistics) const;

    void AddSegment(segmentid_t segmentId);
    void AddSegment(segmentid_t segmentId, const SegmentTopologyInfo& topoInfo);
    void RemoveSegment(segmentid_t segmentId);
    size_t GetSegmentCount() const;
    segmentid_t GetLastSegment() const;
    bool HasSegment(segmentid_t segment) const;

    segmentid_t operator[](size_t n) const;

    versionid_t GetVersionId() const;
    void SetVersionId(versionid_t versionId);
    void IncVersionId();

    void SetLastSegmentId(segmentid_t segId) { mLastSegmentId = segId; }

    void SetTimestamp(int64_t timestamp);
    int64_t GetTimestamp() const;

    void SetLocator(const document::Locator& locator) { mLocator = locator; }
    const document::Locator& GetLocator() const { return mLocator; }

    SegmentIdVec GetSegmentVector();
    const SegmentIdVec& GetSegmentVector() const;

    std::string GetSegmentDirName(segmentid_t segId) const;
    std::string GetNewSegmentDirName(segmentid_t segId) const;

    indexlibv2::framework::LevelInfo& GetLevelInfo();
    const indexlibv2::framework::LevelInfo& GetLevelInfo() const;

    bool IsLegacySegmentDirName() const { return mFormatVersion == 0; }

    uint32_t GetFormatVersion() const { return mFormatVersion; }
    void SetFormatVersion(uint32_t formatVersion);

    bool Load(const std::string& path);
    bool Load(const file_system::DirectoryPtr& directory, const std::string& fileName);

    void Store(const file_system::DirectoryPtr& directory, bool overwrite);
    void TEST_Store(const std::string& path, bool overwrite);

    bool IsValidSegmentDirName(const std::string& segDirName) const;
    std::string GetVersionFileName() const { return GetVersionFileName(mVersionId); }
    std::string GetSchemaFileName() const { return GetSchemaFileName(mSchemaVersionId); }

public:
    static bool IsValidSegmentDirName(const std::string& segDirName, bool isLegacy);
    static segmentid_t GetSegmentIdByDirName(const std::string& segDirName);
    static std::string GetSchemaFileName(schemaid_t schemaId);
    static std::string GetVersionFileName(versionid_t versionId);
    static bool ExtractSchemaIdBySchemaFile(const std::string& schemaFileName, schemaid_t& schemaId);

    void SetSchemaVersionId(schemaid_t schemaId) { mSchemaVersionId = schemaId; }
    schemaid_t GetSchemaVersionId() const { return mSchemaVersionId; }
    void SyncSchemaVersionId(const config::IndexPartitionSchemaPtr& schema);

    void AddDescription(const std::string& key, const std::string& value);
    bool GetDescription(const std::string& key, std::string& value) const;
    void SetDescriptions(const std::map<std::string, std::string>& descs);
    void MergeDescriptions(const Version& version);
    void SetUpdateableSchemaStandards(const config::UpdateableSchemaStandards& standards);
    const config::UpdateableSchemaStandards& GetUpdateableSchemaStandards() const;

    static void SetDefaultVersionFormatNumForTest(uint32_t version);

public:
    bool operator==(const Version& other) const;
    bool operator!=(const Version& other) const;
    // TODO: operator<, operator> no support rt member var
    bool operator<(const Version& other) const;
    bool operator>(const Version& other) const;
    void operator=(const Version& other);

    friend Version operator-(const Version& left, const Version& right);

public:
    Iterator CreateIterator() const;

public:
    static const uint32_t CURRENT_FORMAT_VERSION;
    static const uint32_t INVALID_VERSION_FORMAT_NUM;

private:
    static void Validate(const SegmentIdVec& segmentIds);
    void FillWishList(const file_system::DirectoryPtr& rootDir, std::vector<std::string>* wishFileList,
                      std::vector<std::string>* wishDirList);

private:
    versionid_t mVersionId;
    segmentid_t mLastSegmentId;
    volatile int64_t mTimestamp;
    document::Locator mLocator;
    SegmentIdVec mSegmentIds;
    indexlibv2::framework::LevelInfo mLevelInfo;
    uint32_t mFormatVersion;
    schemaid_t mSchemaVersionId;
    ModifyOpIdVec mOngoingModifyOpIds;
    std::map<std::string, std::string> mDesc;
    SegTemperatureVec mSegTemperatureMetas;
    SegmentStatisticVec mSegStatisticVec;
    config::UpdateableSchemaStandards mUpdateableSchemaStandards;

private:
    friend class VersionTest;
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<Version> VersionPtr;
typedef std::vector<Version> VersionVector;

////////////////////////////////////////////////////////
inline void Version::Clear()
{
    mLevelInfo.Clear();
    mSegmentIds.clear();
    mSegTemperatureMetas.clear();
    mSegStatisticVec.clear();
}

inline void Version::RemoveSegment(segmentid_t segmentId)
{
    SegmentIdVec::iterator it = mSegmentIds.begin();
    for (; it != mSegmentIds.end(); ++it) {
        if (*it == segmentId) {
            break;
        }
    }
    if (it == mSegmentIds.end()) {
        return;
    }
    mSegmentIds.erase(it);
    mLevelInfo.RemoveSegment(segmentId);
    RemoveSegTemperatureMeta(segmentId);
    RemoveSegmentStatistics(segmentId);
}

inline size_t Version::GetSegmentCount() const { return mSegmentIds.size(); }

inline segmentid_t Version::operator[](size_t n) const
{
    assert(n < mSegmentIds.size());
    return mSegmentIds[n];
}

inline segmentid_t Version::GetLastSegment() const { return mLastSegmentId; }

inline Version::Iterator Version::CreateIterator() const { return Iterator(mSegmentIds); }

inline versionid_t Version::GetVersionId() const { return mVersionId; }

inline void Version::SetVersionId(versionid_t versionId) { mVersionId = versionId; }

inline void Version::IncVersionId() { ++mVersionId; }

inline void Version::SetTimestamp(int64_t timestamp) { mTimestamp = timestamp; }

inline int64_t Version::GetTimestamp() const { return mTimestamp; }

inline Version::SegmentIdVec Version::GetSegmentVector() { return mSegmentIds; }

inline const Version::SegmentIdVec& Version::GetSegmentVector() const { return mSegmentIds; }

inline indexlibv2::framework::LevelInfo& Version::GetLevelInfo() { return mLevelInfo; }

inline const indexlibv2::framework::LevelInfo& Version::GetLevelInfo() const { return mLevelInfo; }

inline void Version::operator=(const Version& other)
{
    mVersionId = other.mVersionId;
    MEMORY_BARRIER();
    mLastSegmentId = other.mLastSegmentId;
    mTimestamp = other.mTimestamp;
    mLocator = other.mLocator;
    mSegmentIds = other.mSegmentIds;
    mLevelInfo = other.mLevelInfo;
    mFormatVersion = other.mFormatVersion;
    mSchemaVersionId = other.mSchemaVersionId;
    mOngoingModifyOpIds = other.mOngoingModifyOpIds;
    mDesc = other.mDesc;
    mSegTemperatureMetas = other.mSegTemperatureMetas;
    mSegStatisticVec = other.mSegStatisticVec;
    mUpdateableSchemaStandards = other.mUpdateableSchemaStandards;
}

inline bool Version::operator==(const Version& other) const
{
    return (mVersionId == other.mVersionId) && (mSegmentIds == other.mSegmentIds) && (mTimestamp == other.mTimestamp) &&
           (mLastSegmentId == other.mLastSegmentId) && (mLocator == other.mLocator) &&
           (mSchemaVersionId == other.mSchemaVersionId) && (mOngoingModifyOpIds == other.mOngoingModifyOpIds) &&
           (mDesc == other.mDesc) && (mSegTemperatureMetas == other.mSegTemperatureMetas) &&
           (mSegStatisticVec == other.mSegStatisticVec) &&
           (mUpdateableSchemaStandards == other.mUpdateableSchemaStandards);
}

inline bool Version::operator!=(const Version& other) const { return !((*this) == other); }

inline bool Version::operator<(const Version& other) const { return mVersionId < other.mVersionId; }

inline bool Version::operator>(const Version& other) const { return mVersionId > other.mVersionId; }

}} // namespace indexlib::index_base
