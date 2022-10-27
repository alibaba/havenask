#include "indexlib/partition/test/online_partition_perf_test.h"
#include "indexlib/test/schema_maker.h"
#include <autil/StringUtil.h>
#include "indexlib/test/slow_dump_segment_container.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionPerfTest);

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionPerfTest, TestReopen);

namespace {
class FakeMemoryQuotaController : public util::MemoryQuotaController
{
public:
    FakeMemoryQuotaController()
        : util::MemoryQuotaController(1024*1024*1024)
    {}
    ~FakeMemoryQuotaController() {}

    bool TryAllocate(int64_t quota)
    {
        if (rand() % 13 < 4) { return false; }
        return util::MemoryQuotaController::GetFreeQuota();
    }
};
}

OnlinePartitionPerfTest::OnlinePartitionPerfTest()
{
}

OnlinePartitionPerfTest::~OnlinePartitionPerfTest()
{
}

void OnlinePartitionPerfTest::CaseSetUp()
{
    mTimestamp = 1;
    string field = "pk:string:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    string attr = "long1;";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");

    mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    BuildConfig& buildConfig = mOptions.GetBuildConfig();
    buildConfig.maxDocCount = 10;
}

void OnlinePartitionPerfTest::CaseTearDown()
{
    mPsm.reset();
}

void OnlinePartitionPerfTest::DoWrite()
{
    ThreadPtr reopenThread = Thread::createThread(
            tr1::bind(&OnlinePartitionPerfTest::DoReopen, this));
    ThreadPtr rtThread = Thread::createThread(
            tr1::bind(&OnlinePartitionPerfTest::DoRtBuild, this));
    ThreadPtr incThread = Thread::createThread(
            tr1::bind(&OnlinePartitionPerfTest::DoIncBuild, this));

    incThread.reset();
    rtThread.reset();
    reopenThread.reset();
}

void OnlinePartitionPerfTest::DoReopen()
{
    while (!IsFinished())
    {
        mPsm->Transfer(PE_REOPEN, "", "", "");
        usleep(100);
    }
}

void OnlinePartitionPerfTest::DoRtBuild()
{
    while (!IsFinished())
    {
        string docStr = MakeDocStr();
        PushDoc(docStr);
        ASSERT_TRUE(mPsm->Transfer(BUILD_RT, docStr, "", ""));
    }
}

void OnlinePartitionPerfTest::DoIncBuild()
{
    while (!IsFinished())
    {
        string docs = "";
        for (size_t i = 0; i < 10; ++i)
        {
            string doc = PopDoc();
            if (doc.empty())
            {
                continue;
            }
            docs += doc;
        }
        if (!docs.empty())
        {
            usleep(100);
            ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_REOPEN, docs, "", ""));
            size_t retryTimes = 0;
            PsmEvent event = PE_REOPEN;
            while(!mPsm->Transfer(event, "", "", ""))
            {
                retryTimes++;
                IE_LOG(ERROR, "retry times %lu", retryTimes);
            }
        }
    }
}

void OnlinePartitionPerfTest::DoRead(int* errCode)
{
    while (!IsFinished())
    {
        mPsm->Transfer(QUERY, "", "index1:hello", "");
        usleep(100);
    }
}

void OnlinePartitionPerfTest::TestReopenWithAsyncDump()
{
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController =
        util::MemoryQuotaControllerPtr(new FakeMemoryQuotaController());
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    SlowDumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(100));
    mPsm.reset(new PartitionStateMachine(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource,
                   dumpContainer));
    ASSERT_TRUE(mPsm->Init(mSchema, mOptions, GET_TEST_DATA_PATH()));


    ASSERT_TRUE(mPsm->Transfer(BUILD_RT, MakeDocStr(), "", ""));
    DoMultiThreadTest(1, 500);
}

void OnlinePartitionPerfTest::TestReopen()
{
    //result not care
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController =
        util::MemoryQuotaControllerPtr(new FakeMemoryQuotaController());
    mPsm.reset(new PartitionStateMachine(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource));
    ASSERT_TRUE(mPsm->Init(mSchema, mOptions, GET_TEST_DATA_PATH()));


    ASSERT_TRUE(mPsm->Transfer(BUILD_RT, MakeDocStr(), "", ""));
    DoMultiThreadTest(1, 500);
}

string OnlinePartitionPerfTest::MakeDocStr()
{
    string tsStr = StringUtil::toString(mTimestamp++);
    string docStr = string("cmd=add,string1=hello,ts=") + tsStr + ",pk=" + tsStr + ";";
    string dupAddPk = StringUtil::toString(rand() % mTimestamp);
    docStr += string("cmd=add,string1=hello,ts=") + tsStr + ",pk=" + dupAddPk + ";";
    string updatePk = StringUtil::toString(rand() % mTimestamp);
    docStr += string("cmd=update_field,long1=") + updatePk + ",ts=" + tsStr + ",pk=" + updatePk + ";";

    if (rand() % 25 == 0)
    {
        string deletePk = StringUtil::toString(rand() % mTimestamp);
        docStr += string("cmd=delete,ts=") + tsStr + ",pk=" + deletePk + ";";
    }
    return docStr;
}

IE_NAMESPACE_END(partition);

