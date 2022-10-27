#ifndef __INDEXLIB_DEMO_BUILDING_SEGMENT_READER_H
#define __INDEXLIB_DEMO_BUILDING_SEGMENT_READER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/building_segment_reader.h"

DECLARE_REFERENCE_CLASS(testlib, Result);
IE_NAMESPACE_BEGIN(table);

class DemoBuildingSegmentReader : public BuildingSegmentReader
{
public:
    DemoBuildingSegmentReader(std::shared_ptr<autil::ThreadMutex> dataLock,
                              std::shared_ptr<std::map<std::string, std::string>> data);
    ~DemoBuildingSegmentReader();
public:
    testlib::ResultPtr Search(const std::string& query) const;

private:
    std::shared_ptr<autil::ThreadMutex> mDataLock;
    std::shared_ptr<std::map<std::string, std::string>> mData;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoBuildingSegmentReader);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_DEMO_BUILDING_SEGMENT_READER_H
