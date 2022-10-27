#include "indexlib/partition/perf_test/partition_big_data_perftest.h"
#include <autil/StringUtil.h>
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionBigDataPerfTest);

PartitionBigDataPerfTest::PartitionBigDataPerfTest()
{
}

PartitionBigDataPerfTest::~PartitionBigDataPerfTest()
{
}

void PartitionBigDataPerfTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "string1:string;summary1:string;";
    string index = "index1:string:string1;";
    string attr = "";
    string summary = "summary1;";
    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    mOptions = IndexPartitionOptions();
    mOptions.GetOnlineConfig().maxRealtimeMemSize = 11 * 1024 *1024; 
    BuildConfig& buildConfig = mOptions.GetBuildConfig();
    buildConfig.buildTotalMemory = 10 * 1024 *1024;
}

void PartitionBigDataPerfTest::CaseTearDown()
{
}

void PartitionBigDataPerfTest::TestSummaryInBuildingSegmentWithBigData()
{
    PartitionStateMachine psm;
    size_t docNum = 96;
    size_t docSize = 64 * 1024 * 1024;
    {
        string docString;
        MakeData(docSize, docNum, docString);

        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    }
    CheckSummary(psm, docSize, docNum);
}

string PartitionBigDataPerfTest::MakeSummaryStr(size_t docSize, size_t docId)
{
    string strDocId = StringUtil::toString(docId);
    assert(strDocId.size() < docSize);
    size_t docIdNum = docSize / strDocId.size();
    string docString;
    docString.reserve(docSize);
    for (size_t i = 0; i < docIdNum; ++i)
    {
        docString.append(strDocId);
    }
    size_t sharpNum = docSize % strDocId.size();
    string paddingStr = string(sharpNum, '#');
    docString.append(paddingStr);
    return docString;
}

void PartitionBigDataPerfTest::MakeData(size_t docSize, size_t docNum, string& docString)
{
    size_t reservedSize = docNum * (docSize + 40);
    docString.reserve(reservedSize);
    for (size_t docId = 0; docId < docNum; ++docId)
    {
        string strDocId = StringUtil::toString(docId);
        assert(strDocId.size() < docSize);
        string paddingStr = string(docSize - strDocId.size(), '#');
        string singleDocStr = "cmd=add,string1=1,ts=1,summary1=" + 
                              MakeSummaryStr(docSize, docId) + 
                              ";";
        docString.append(singleDocStr);
    }
}

void PartitionBigDataPerfTest::CheckSummary(
        PartitionStateMachine& psm, size_t docSize, size_t docNum)
{
    for (size_t docId = 0; docId < docNum; ++docId)
    {
        string query = "__docid:" + StringUtil::toString(docId);
        string expectResult = "summary1=" + MakeSummaryStr(docSize, docId) + ";";
        ASSERT_TRUE(psm.Transfer(QUERY, "", query, expectResult));
    }
}

IE_NAMESPACE_END(partition);

