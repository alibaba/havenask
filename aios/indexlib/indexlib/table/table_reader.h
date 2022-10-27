#ifndef __INDEXLIB_TABLE_READER_H
#define __INDEXLIB_TABLE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/building_segment_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(table);

/*
    increase interface id when sub class interface has changed
    user should make sure that:
    sub class with different interface id should be in different namespaces
    
    eg:
    namespace N { 
       class SubReader : public TableReader   // DEFAULT_INTERFACE_ID;
       {};  
    }

    namespace N {
    namespace v1 {
       class SubReader : public TableReader
       {
          DECLARE_READER_INTERFACE_ID(1);
       };
    }
    }
*/

#define DECLARE_READER_INTERFACE_ID(id)                                 \
    protected:                                                          \
    void InitInterfaceId() override { mInterfaceId = id; }              

class BuiltSegmentReader
{
public:
    BuiltSegmentReader() {}
    virtual ~BuiltSegmentReader() {}
public:
    virtual bool Open(const table::SegmentMetaPtr& segMeta)
    {
        return false;
    }
};
DEFINE_SHARED_PTR(BuiltSegmentReader);

class TableReader
{
public:
    TableReader();
    virtual ~TableReader();
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options);

    virtual bool DoInit();

    virtual bool Open(const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
                      const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
                      const BuildingSegmentReaderPtr& buildingSegmentReader,
                      int64_t incVersionTimestamp) = 0;

    virtual size_t EstimateMemoryUse(
        const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
        const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,        
        const BuildingSegmentReaderPtr& buildingSegmentReader,
        int64_t incVersionTimestamp) const = 0;

    interfaceid_t GetInterfaceId() const { return mInterfaceId; }
    void SetSegmentDependency(const std::vector<BuildingSegmentReaderPtr>& inMemSegments);
    virtual void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo) {
        mForceSeekInfo = forceSeekInfo;
    }

public:
    virtual bool SupportSegmentLevelReader() const { return false; } 

    virtual BuiltSegmentReaderPtr CreateBuiltSegmentReader() const 
    {
        return BuiltSegmentReaderPtr();
    }

    virtual bool Open(const std::vector<BuiltSegmentReaderPtr>& builtSegments,
                      const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
                      const BuildingSegmentReaderPtr& buildingSegmentReader,
                      int64_t incVersionTimestamp) { return false; }

    virtual BuiltSegmentReaderPtr GetSegmentReader(segmentid_t segId) const {
        return BuiltSegmentReaderPtr();
    }

public:
    virtual bool TEST_query(const std::string& query, std::string& result, std::string& errorMsg)
    {
        errorMsg = "not supported";
        return false;
    }

protected:
    virtual void InitInterfaceId() { mInterfaceId = DEFAULT_INTERFACE_ID; }
    
protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    interfaceid_t mInterfaceId;
    std::pair<int64_t, int64_t> mForceSeekInfo;
private:
    std::vector<BuildingSegmentReaderPtr> mDependentInMemSegments;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableReader);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_READER_H
