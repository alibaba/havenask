#include "indexlib/partition/perf_test/multi_thread_dump_perftest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, MultiThreadDumpPerfTest);

MultiThreadDumpPerfTest::MultiThreadDumpPerfTest()
{
}

MultiThreadDumpPerfTest::~MultiThreadDumpPerfTest()
{
}

void MultiThreadDumpPerfTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void MultiThreadDumpPerfTest::CaseTearDown()
{
}

void MultiThreadDumpPerfTest::TestSingleThreadDump()
{
    DoMultiThreadDump(1, true);
    DoMultiThreadDump(1, false);
}

void MultiThreadDumpPerfTest::TestMultiThreadDump()
{
    DoMultiThreadDump(8, true);
    DoMultiThreadDump(8, false);
}

void MultiThreadDumpPerfTest::DoMultiThreadDump(
        uint32_t threadCount, bool enablePackageFile)
{
    TearDown();
    SetUp();

    const size_t DOC_COUNT_FOR_ONE_SEGMENT = 2;
    const size_t TOTAL_DOC_COUNT = 100;
    const size_t INDEX_FIELD_COUNT = 16;
    const size_t TERM_COUNT = 16;

    string field = "";
    string index = "";
    for (size_t i = 0; i < INDEX_FIELD_COUNT; ++i)
    {
        string indexNo = StringUtil::toString(i);
        field += "string" + indexNo + ":string;";
        index += "index" + indexNo + ":string:string" + indexNo + ";";
    }

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    INDEXLIB_TEST_TRUE(schema);

    string docString = "";
    for (size_t docIndex = 0; docIndex < TOTAL_DOC_COUNT; ++docIndex)
    {
        string termStr = "";
        for (size_t i = 0; i < TERM_COUNT; ++i)
        {
            termStr += StringUtil::toString(i + TERM_COUNT * docIndex) + " ";
        }

        docString += "cmd=add,";
        for (size_t i = 0; i < INDEX_FIELD_COUNT; ++i)
        {
            string indexNo = StringUtil::toString(i);
            docString += "string" + indexNo + "=" + termStr + ",";
        }
        docString += "ts=1;";
    } 
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    BuildConfig& buildConfig = options.GetBuildConfig();
    buildConfig.dumpThreadCount = threadCount;
    buildConfig.enablePackageFile = enablePackageFile;
    buildConfig.maxDocCount = DOC_COUNT_FOR_ONE_SEGMENT;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
}

IE_NAMESPACE_END(partition);

