#ifndef __INDEXLIB_VERSION2_H
#define __INDEXLIB_VERSION2_H

#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/index_base/index_meta/level_info.h"
#include "indexlib/document/locator.h"
#include <tr1/memory>

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, SegmentTopologyInfo);

IE_NAMESPACE_BEGIN(index_base);

class Version : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<segmentid_t> SegmentIdVec;
    typedef std::vector<schema_opid_t> ModifyOpIdVec;
    
    class Iterator
    {
    public:
        Iterator(const SegmentIdVec& vec)
            : mVec(vec)
            , mIter(vec.begin())
        { }
        Iterator(const Iterator& iter)
            : mVec(iter.mVec)
            , mIter(iter.mVec.begin())
        { }
        bool HasNext() const { return (mIter != mVec.end()); }
        segmentid_t Next() { return (*mIter++); }
    private:
        const SegmentIdVec& mVec;
        SegmentIdVec::const_iterator mIter;
    };

public:
    Version();
    Version(versionid_t versionId);
    Version(versionid_t versionId, int64_t timestamp);
    Version(const Version& other);
    ~Version();

public:
    void Jsonize(JsonWrapper& json) override;

    void FromString(const std::string& str);
    std::string ToString() const;

    void Clear();

    void SetOngoingModifyOperations(const ModifyOpIdVec& opIds) { mOngoingModifyOpIds = opIds; }
    const ModifyOpIdVec& GetOngoingModifyOperations() const { return mOngoingModifyOpIds; }
    
    void AddSegment(segmentid_t segmentId);
    void AddSegment(segmentid_t segmentId, const SegmentTopologyInfo& topoInfo);
    void RemoveSegment(segmentid_t segmentId);
    size_t GetSegmentCount() const;
    segmentid_t GetLastSegment() const;
    bool HasSegment(segmentid_t segment) const;

    segmentid_t operator [] (size_t n) const;

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

    LevelInfo& GetLevelInfo();
    const LevelInfo& GetLevelInfo() const;

    bool IsLegacySegmentDirName() const { return mFormatVersion == 0; }
    
    uint32_t GetFormatVersion() const { return mFormatVersion; }
    void SetFormatVersion(uint32_t formatVersion);
    
    bool Load(const std::string& path);
    bool Load(const file_system::DirectoryPtr& directory,
              const std::string& fileName);

    void Store(const std::string& dir, bool overwrite) const;
    void Store(const file_system::DirectoryPtr& directory, bool overwrite = false);
    
    bool IsValidSegmentDirName(const std::string& segDirName) const;
    std::string GetVersionFileName() const { return GetVersionFileName(mVersionId); }
    std::string GetSchemaFileName() const { return GetSchemaFileName(mSchemaVersionId); }
    
public:
    static bool IsValidSegmentDirName(const std::string& segDirName, bool isLegacy);
    static segmentid_t GetSegmentIdByDirName(const std::string& segDirName);
    static std::string GetSchemaFileName(schemavid_t schemaId);
    static std::string GetVersionFileName(versionid_t versionId);
    static bool ExtractSchemaIdBySchemaFile(
            const std::string& schemaFileName, schemavid_t &schemaId);

    void SetSchemaVersionId(schemavid_t schemaId) { mSchemaVersionId = schemaId; }
    schemavid_t GetSchemaVersionId() const { return mSchemaVersionId; }
    void SyncSchemaVersionId(const config::IndexPartitionSchemaPtr& schema);

public:
    bool operator == (const Version& other) const;
    bool operator != (const Version& other) const;
    //TODO: operator<, operator> no support rt member var
    bool operator < (const Version& other) const;
    bool operator > (const Version& other) const;
    void operator = (const Version& other);

    friend Version operator - (const Version& left, const Version& right);

public:
    Iterator CreateIterator() const;

public:    
    static const uint32_t CURRENT_FORMAT_VERSION;

private:
    static void Validate(const SegmentIdVec& segmentIds);

private:
    versionid_t mVersionId;
    segmentid_t mLastSegmentId;
    volatile int64_t mTimestamp;
    document::Locator mLocator;
    SegmentIdVec mSegmentIds;
    LevelInfo mLevelInfo;
    uint32_t mFormatVersion;
    schemavid_t mSchemaVersionId;
    ModifyOpIdVec mOngoingModifyOpIds;
    
private:
    friend class VersionTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<Version> VersionPtr;
typedef std::vector<Version> VersionVector;

////////////////////////////////////////////////////////
inline void Version::Clear()
{
    mLevelInfo.Clear();
    mSegmentIds.clear();
}

inline void Version::RemoveSegment(segmentid_t segmentId)
{
    SegmentIdVec::iterator it = mSegmentIds.begin();
    for (; it != mSegmentIds.end(); ++it)
    {
        if (*it == segmentId)
        {
            break;
        }
    }
    if (it == mSegmentIds.end())
    {
        return;
    }
    mSegmentIds.erase(it);
    mLevelInfo.RemoveSegment(segmentId);
}

inline size_t Version::GetSegmentCount() const
{
    return mSegmentIds.size();
}

inline segmentid_t Version::operator [] (size_t n) const
{
    assert(n < mSegmentIds.size());
    return mSegmentIds[n];
}

inline segmentid_t Version::GetLastSegment() const 
{
    return mLastSegmentId;
}

inline Version::Iterator Version::CreateIterator() const
{
    return Iterator(mSegmentIds);
}

inline versionid_t Version::GetVersionId() const
{
    return mVersionId;
}

inline void Version::SetVersionId(versionid_t versionId)
{
    mVersionId = versionId;
}

inline void Version::IncVersionId()
{
    ++mVersionId;
}

inline void Version::SetTimestamp(int64_t timestamp)
{
    mTimestamp = timestamp;
}

inline int64_t Version::GetTimestamp() const
{
    return mTimestamp;
}

inline Version::SegmentIdVec Version::GetSegmentVector()
{
    return mSegmentIds;
}

inline const Version::SegmentIdVec& Version::GetSegmentVector() const
{
    return mSegmentIds;
}

inline LevelInfo& Version::GetLevelInfo()
{
    return mLevelInfo;
}

inline const LevelInfo& Version::GetLevelInfo() const
{
    return mLevelInfo;
}

inline void Version::operator = (const Version& other)
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
}

inline bool Version::operator == (const Version& other) const
{
    return  (mVersionId == other.mVersionId) &&
        (mSegmentIds == other.mSegmentIds) &&
        (mTimestamp == other.mTimestamp) &&
        (mLastSegmentId == other.mLastSegmentId) &&
        (mLocator == other.mLocator) &&
        (mSchemaVersionId == other.mSchemaVersionId) &&
        (mOngoingModifyOpIds == other.mOngoingModifyOpIds);
}

inline bool Version::operator != (const Version& other) const
{
    return ! ((*this) == other);
}

inline bool Version::operator < (const Version& other) const
{
    return mVersionId < other.mVersionId;
}

inline bool Version::operator > (const Version& other) const
{
    return mVersionId > other.mVersionId;
}

/////////////////////////////////////////
inline std::ostream& operator <<(std::ostream& os, const Version& version)
{
    os << version.ToString();
    return os;
}

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_VERSION2_H
