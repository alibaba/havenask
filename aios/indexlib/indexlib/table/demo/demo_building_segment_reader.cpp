#include "indexlib/table/demo/demo_building_segment_reader.h"
#include "indexlib/testlib/result.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(testlib);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoBuildingSegmentReader);

DemoBuildingSegmentReader::DemoBuildingSegmentReader(
    std::shared_ptr<autil::ThreadMutex> dataLock,
    std::shared_ptr<std::map<std::string, std::string>> data)
    : BuildingSegmentReader()
    , mDataLock(dataLock)
    , mData(data)
{
}

DemoBuildingSegmentReader::~DemoBuildingSegmentReader() 
{
}

ResultPtr DemoBuildingSegmentReader::Search(const string& query) const
{
    ResultPtr result(new Result());
    string values;
    {
        ScopedLock lock(*mDataLock);
        const auto& keyIter = mData->find(query);
        if (keyIter == mData->end())
        {
            return result;
        }
        values = keyIter->second;
    }
    RawDocumentPtr rawDoc(new RawDocument());
    StringTokenizer st(values,
                       ":", StringTokenizer::TOKEN_TRIM | 
                       StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2)
    {
        return result;
    }
    rawDoc->SetField("cfield1", st[0]);
    rawDoc->SetField("cfield2", st[1]);
    result->AddDoc(rawDoc);
    return result;
}


IE_NAMESPACE_END(table);

