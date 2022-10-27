#include "indexlib/table/demo/demo_table_reader.h"
#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/table/demo/demo_building_segment_reader.h"
#include "indexlib/testlib/result.h"
#include "indexlib/table/table_reader.h"
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoTableReader);

DemoTableReader::DemoTableReader(
    const util::KeyValueMap& parameters)
    : SimpleTableReader()
{
}

DemoTableReader::~DemoTableReader() 
{
}

bool DemoTableReader::Open(const vector<SegmentMetaPtr>& builtSegmentMetas,
                           const vector<BuildingSegmentReaderPtr>& dumpingSegments,
                           const BuildingSegmentReaderPtr& buildingSegmentReader,
                           int64_t incTimestamp)
{
    mSegDatas.reserve(builtSegmentMetas.size());
    for (auto& segMeta : builtSegmentMetas)
    {
        try
        {
            string fileContent;
            segMeta->segmentDataDirectory->Load(DemoTableWriter::DATA_FILE_NAME , fileContent);
            map<string, string> segData;
            autil::legacy::FromJsonString(segData, fileContent);
            mSegDatas.push_back(segData);
        }
        catch(const autil::legacy::ExceptionBase& e)
        {
            IE_LOG(ERROR, "fail to load index file [%s] in segment[%s], error: [%s]",
                   DemoTableWriter::DATA_FILE_NAME.c_str(),
                   segMeta->segmentDataBase.GetSegmentDirName().c_str(), e.what());
            return false;
        }
    }
    mInMemSegments.reserve(dumpingSegments.size() + 1);
    for (auto &segment : dumpingSegments)
    {
        mInMemSegments.emplace_back(DYNAMIC_POINTER_CAST(DemoBuildingSegmentReader, segment));
    }
    mInMemSegments.emplace_back(DYNAMIC_POINTER_CAST(DemoBuildingSegmentReader, buildingSegmentReader));
    return true;
}

ResultPtr DemoTableReader::Search(const string& query) const
{
    ResultPtr result(new Result());
    if (mInMemSegments.size())
    {
        for (auto it = mInMemSegments.rbegin(); it != mInMemSegments.rend(); ++it)
        {
            ResultPtr buildingResult = (*it)->Search(query);
            if (buildingResult->GetDocCount() > 0)
            {
                return buildingResult;
            }
        }
    }
    for (auto it = mSegDatas.rbegin(); it != mSegDatas.rend(); ++it)
    {
        const auto& segData = *it;
        const auto& keyIter = segData.find(query);
        if (keyIter != segData.end())
        {
            string values = keyIter->second;
            RawDocumentPtr rawDoc(new RawDocument());
            StringTokenizer st(values,
                               ":", StringTokenizer::TOKEN_TRIM | 
                               StringTokenizer::TOKEN_IGNORE_EMPTY);
            if (st.getNumTokens() != 2)
            {
                continue;
            }
            rawDoc->SetField("cfield1", st[0]);
            rawDoc->SetField("cfield2", st[1]);
            result->AddDoc(rawDoc);
            break;
        }
    }
    return result;
}

size_t DemoTableReader::EstimateMemoryUse(
    const vector<SegmentMetaPtr>& builtSegmentMetas,
    const vector<BuildingSegmentReaderPtr>& dumpingSegments,
    const BuildingSegmentReaderPtr& buildingSegmentReader,
    int64_t incTimestamp) const
{
    size_t memUse = 0u;
    for (auto& segMeta : builtSegmentMetas)
    {
        try
        {
            memUse += segMeta->segmentDataDirectory->GetFileLength(DemoTableWriter::DATA_FILE_NAME);
        }
        catch(const autil::legacy::ExceptionBase& e)
        {
            IE_LOG(ERROR, "fail to get index file [%s] length in segment[%s], error: [%s]",
                   DemoTableWriter::DATA_FILE_NAME.c_str(),
                   segMeta->segmentDataBase.GetSegmentDirName().c_str(), e.what());
            throw;
        }
    }
    return memUse;
}

IE_NAMESPACE_END(table);

