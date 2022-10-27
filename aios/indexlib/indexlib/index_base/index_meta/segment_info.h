#ifndef __INDEXLIB_SEGMENT_INFOS_H
#define __INDEXLIB_SEGMENT_INFOS_H

#include <typeinfo>
#include <vector>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"
#include "indexlib/index_define.h"
#include "indexlib/document/locator.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

struct SegmentInfo;
typedef std::vector<SegmentInfo> SegmentInfos;

struct SegmentInfo : public autil::legacy::Jsonizable
{
    static const uint32_t INVALID_SHARDING_ID = std::numeric_limits<uint32_t>::max();
    static const uint32_t INVALID_COLUMN_COUNT = std::numeric_limits<uint32_t>::max();
    uint64_t volatile docCount;
    document::ThreadSafeLocator locator;
    int64_t timestamp;
    bool mergedSegment;
    uint32_t maxTTL;
    uint32_t shardingColumnCount;
    uint32_t shardingColumnId;
    schemavid_t schemaId;

    SegmentInfo()
    {
        Init();
    }

    void Init()
    {
        docCount = 0;
        timestamp = INVALID_TIMESTAMP;
        mergedSegment = false;
        maxTTL = 0;
        shardingColumnCount = 1;
        shardingColumnId = INVALID_SHARDING_ID;
        schemaId = DEFAULT_SCHEMAID;
    }

    bool operator == (const SegmentInfo& other) const
    {
        if (this == &other)
            return true;
        return ((other.docCount == docCount)
                && (other.locator == locator)
                && (other.timestamp == timestamp)
                && (other.mergedSegment == mergedSegment)
                && (other.maxTTL == maxTTL)
                && (other.shardingColumnCount == shardingColumnCount)
                && (other.shardingColumnId == shardingColumnId)
                && (other.schemaId == schemaId));
    }

    bool operator != (const SegmentInfo& other) const
    {
        return !(*this == other);
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
    std::string ToString(bool compactFormat = false) const
    {
        return ToJsonString(*this, compactFormat);
    }
    void FromString(const std::string& str)
    {
        FromJsonString(*this, str);
    }
    
    bool Load(const std::string& path);
    bool Load(const file_system::DirectoryPtr& dir);

    void Store(const std::string& path) const;
    void Store(const file_system::DirectoryPtr& directory) const;
    void StoreIgnoreExist(const std::string& path) const;

    void Update(const document::DocumentPtr& doc);

    // whether has multi sharding data or not in current segment 
    bool HasMultiSharding() const;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentInfo);

//////////////////////////////////////////////
inline std::ostream& operator <<(std::ostream& os, const SegmentInfo& segInfo)
{
    os << segInfo.ToString();
    return os;
}

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_INFOS_H
