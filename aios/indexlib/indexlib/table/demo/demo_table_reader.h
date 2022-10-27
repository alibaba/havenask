#ifndef __INDEXLIB_DEMO_TABLE_READER_H
#define __INDEXLIB_DEMO_TABLE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/test/simple_table_reader.h"

DECLARE_REFERENCE_CLASS(table, DemoBuildingSegmentReader);
IE_NAMESPACE_BEGIN(table);

class DemoTableReader : public test::SimpleTableReader
{
public:
    DemoTableReader(const util::KeyValueMap& parameters);
    ~DemoTableReader();
public:
    bool Open(const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
              const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
              const BuildingSegmentReaderPtr& buildingSegmentReader,
              int64_t incTimestamp) override;

    testlib::ResultPtr Search(const std::string& query) const override; 

    size_t EstimateMemoryUse(
        const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
        const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
        const BuildingSegmentReaderPtr& buildingSegmentReader,
        int64_t incTimestamp) const override;

private:
    std::vector<std::map<std::string, std::string>> mSegDatas;
    std::vector<DemoBuildingSegmentReaderPtr> mInMemSegments;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoTableReader);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_DEMO_TABLE_READER_H
