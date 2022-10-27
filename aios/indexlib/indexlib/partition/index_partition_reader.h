#ifndef __INDEXLIB_INDEX_PARTITION_READER_H
#define __INDEXLIB_INDEX_PARTITION_READER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include <autil/AtomicCounter.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index/normal/index_metrics_base.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, IndexReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderContainer);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, KKVReader);
DECLARE_REFERENCE_CLASS(index, KVReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(index_base, DimensionDescription);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(common, IndexLocator);

IE_NAMESPACE_BEGIN(partition);

class PartitionVersion
{
public:
    PartitionVersion(const index_base::Version& version = INVALID_VERSION,
                     segmentid_t buildingSegId = INVALID_SEGMENTID,
                     segmentid_t lastLinkRtSegId = INVALID_SEGMENTID)
        : mVersion(version)
        , mBuildingSegmentId(buildingSegId)
        , mLastLinkRtSegId(lastLinkRtSegId)
    {}

    bool operator ==(const PartitionVersion& other) const
    {
        return mVersion == other.mVersion &&
            mBuildingSegmentId == other.mBuildingSegmentId &&
            mLastLinkRtSegId == other.mLastLinkRtSegId;
    }

    bool operator !=(const PartitionVersion& other) const
    { return !(*this == other); }

    segmentid_t GetLastLinkRtSegmentId() const
    { return mLastLinkRtSegId; }

    uint64_t GetHashId() const;
    
    bool IsUsingSegment(segmentid_t segId, TableType type) const
    {
        if (mVersion.HasSegment(segId))
        {
            return true;
        }

        if (type == tt_kv)
        {
            // kv table will use data directly in building segment 
            return mBuildingSegmentId == segId;
        }
        return false;
    }
    
private:
    index_base::Version mVersion;                // built segments
    segmentid_t mBuildingSegmentId;  // building segment
    segmentid_t mLastLinkRtSegId;    // last linked realtime segment id (open in local storage)
};
    
class IndexPartitionReader
{
public:
    typedef index::IndexMetricsBase::AccessCounterMap AccessCounterMap;

public:
    IndexPartitionReader() {}
    IndexPartitionReader(config::IndexPartitionSchemaPtr schema)
        : mSchema(schema)
        , mPartitionVersionHashId(0)
    {}
    virtual ~IndexPartitionReader() {}

public:
    virtual void Open(const index_base::PartitionDataPtr& partitionData) = 0;

    virtual const index::SummaryReaderPtr& GetSummaryReader() const = 0;

    virtual const index::AttributeReaderPtr& GetAttributeReader(
            const std::string& field) const = 0;
    
    virtual const index::PackAttributeReaderPtr& GetPackAttributeReader(
        const std::string& packAttrName) const = 0;

    virtual const index::PackAttributeReaderPtr& GetPackAttributeReader(
        packattrid_t packAttrId) const = 0;

    virtual const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const = 0;
    
    virtual index::IndexReaderPtr GetIndexReader() const = 0;
    virtual const index::IndexReaderPtr& GetIndexReader(
            const std::string& indexName) const = 0;
    virtual const index::IndexReaderPtr& GetIndexReader(indexid_t indexId) const = 0;

    virtual const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const = 0;

    virtual const index::KKVReaderPtr& GetKKVReader(regionid_t regionId = DEFAULT_REGIONID) const
    { assert(false); return RET_EMPTY_KKV_READER; }

    virtual const index::KKVReaderPtr& GetKKVReader(const std::string& regionName) const
    { assert(false); return RET_EMPTY_KKV_READER; }

    virtual const index::KVReaderPtr& GetKVReader(regionid_t regionId = DEFAULT_REGIONID) const
    { assert(false); return RET_EMPTY_KV_READER; }

    virtual const index::KVReaderPtr& GetKVReader(const std::string& regionName) const
    { assert(false); return RET_EMPTY_KV_READER; }

    virtual const table::TableReaderPtr& GetTableReader() const
    {
        static table::TableReaderPtr nullReader;
        return nullReader; 
    }

    virtual index_base::Version GetVersion() const = 0;
    virtual index_base::Version GetOnDiskVersion() const = 0;

    virtual const index::DeletionMapReaderPtr& GetDeletionMapReader() const = 0;

    virtual index::PartitionInfoPtr GetPartitionInfo() const = 0;

    virtual IndexPartitionReaderPtr GetSubPartitionReader() const = 0;

    virtual bool GetSortedDocIdRanges(
        const std::vector<index_base::DimensionDescriptionPtr>& dimensions,
        const DocIdRange& rangeLimits,
        DocIdRangeVector& resultRanges) const = 0;
    
    virtual bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
        size_t wayIdx, DocIdRangeVector& ranges) const
    {
        return false;
    }

    virtual bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
        std::vector<DocIdRangeVector>& rangeVectors) const
    {
        return false;
    }    

    virtual const AccessCounterMap& GetIndexAccessCounters() const = 0;

    virtual const AccessCounterMap& GetAttributeAccessCounters() const = 0;
    config::IndexPartitionSchemaPtr& GetSchema() { return mSchema; }
    virtual versionid_t GetIncVersionId() const;

    virtual index_base::PartitionDataPtr GetPartitionData() const;

    virtual bool IsUsingSegment(segmentid_t segmentId) const;
    
    const PartitionVersion& GetPartitionVersion() const
    { return mPartitionVersion; }

    uint64_t GetPartitionVersionHashId() const
    { return mPartitionVersionHashId; }

    
    int64_t GetLatestDataTimestamp() const;
    
public:
    void TEST_SetPartitionVersion(const PartitionVersion& partVersion)
    {
        mPartitionVersion = partVersion;
    }

protected:
    virtual void InitPartitionVersion(const index_base::PartitionDataPtr& partitionData,
                                      segmentid_t lastLinkSegId);    
protected:
    config::IndexPartitionSchemaPtr mSchema;
    uint64_t mPartitionVersionHashId;
    PartitionVersion mPartitionVersion;

protected:
    static index::SummaryReaderPtr RET_EMPTY_SUMMARY_READER;
    static index::PrimaryKeyIndexReaderPtr RET_EMPTY_PRIMARY_KEY_READER;
    static index::DeletionMapReaderPtr RET_EMPTY_DELETIONMAP_READER;
    static index::IndexReaderPtr RET_EMPTY_INDEX_READER;
    static index::KKVReaderPtr RET_EMPTY_KKV_READER;
    static index::KVReaderPtr RET_EMPTY_KV_READER;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_PARTITION_READER_H
