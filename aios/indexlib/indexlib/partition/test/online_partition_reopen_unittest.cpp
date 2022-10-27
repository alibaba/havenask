#include <autil/StringUtil.h>
#include "indexlib/partition/test/online_partition_reopen_unittest.h"
#include "indexlib/partition/test/fake_memory_quota_controller.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
using namespace std::placeholders;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionReopenTest);

OnlinePartitionReopenTest::OnlinePartitionReopenTest()
{
}

OnlinePartitionReopenTest::~OnlinePartitionReopenTest()
{
}

void OnlinePartitionReopenTest::CaseSetUp()
{
    string field = "pk:string:pk;long1:uint32;updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk";
    string attr = "long1;updatable_multi_long;";
    string summary = "pk";
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema, field, index, attr, summary);
    mFullDocs = "cmd=add,pk=0,long1=0,updatable_multi_long=0,ts=0;"
                "cmd=add,pk=1,long1=1,updatable_multi_long=1,ts=0;"
                "cmd=add,pk=2,long1=2,updatable_multi_long=2,ts=0;"
                "cmd=add,pk=3,long1=3,updatable_multi_long=3,ts=0;"
                "cmd=add,pk=4,long1=4,updatable_multi_long=4,ts=0;"
                "cmd=add,pk=5,long1=5,updatable_multi_long=5,ts=0;";

    //repeat add, update, delete
    mRtDocs = "cmd=add,pk=0,long1=10,updatable_multi_long=10,ts=1;"
              "cmd=update_field,pk=1,long1=11,updatable_multi_long=11,ts=2;"
              "cmd=delete,pk=2,ts=3;"
              "cmd=add,pk=3,long1=13,updatable_multi_long=13,ts=4;"
              "cmd=update_field,pk=4,long1=14,updatable_multi_long=14,ts=5;"
              "cmd=delete,pk=5,ts=6;";

    //inc cover
    mIncDocs = "cmd=add,pk=0,long1=20,updatable_multi_long=20,ts=3;"
               "cmd=update_field,pk=1,long1=21,updatable_multi_long=21,ts=3;"
               "cmd=add,pk=2,long1=22,updatable_multi_long=22,ts=3;"
               "cmd=add,pk=3,long1=23,updatable_multi_long=23,ts=3;"
               "cmd=update_field,pk=4,long1=24,updatable_multi_long=24,ts=3;";

    mRtDocs1 = "cmd=add,pk=0,long1=30,updatable_multi_long=30,ts=1;"
               "cmd=update_field,pk=1,long1=31,updatable_multi_long=31,ts=2;"
               "cmd=add,pk=2,long1=32,updatable_multi_long=32,ts=3;"
               "cmd=add,pk=3,long1=33,updatable_multi_long=33,ts=4;"
               "cmd=update_field,pk=4,long1=34,updatable_multi_long=34,ts=5;";

    mRtDocs2 = "cmd=add,pk=0,long1=30,updatable_multi_long=30,ts=4;"
               "cmd=update_field,pk=1,long1=31,updatable_multi_long=31,ts=4;"
               "cmd=add,pk=2,long1=32,updatable_multi_long=32,ts=4;"
               "cmd=add,pk=3,long1=33,updatable_multi_long=33,ts=4;"
               "cmd=update_field,pk=4,long1=34,updatable_multi_long=34,ts=5;"
               "cmd=add,pk=6,long1=66,updatable_multi_long=66,ts=5;";

    // KV
    mKVFullDocs = "cmd=add,key=1,value=1,ts=101000000;";

    mKVRtDocs0 = "cmd=add,key=3,value=3,ts=103000000;";

    mKVRtDocs1 = "cmd=add,key=4,value=4,ts=105000000;"
                        "cmd=add,key=6,value=6,ts=107000000;";

    mKVIncDocs = "cmd=add,key=3,value=13,ts=106000000;"
                        "cmd=add,key=7,value=7,ts=106000000;";

    mKVRtDocs2 = "cmd=add,key=8,value=8,ts=115000000;";
}

void OnlinePartitionReopenTest::CaseTearDown()
{
}

void OnlinePartitionReopenTest::DoTest(std::function<void(int64_t, int64_t, bool&, bool&)> tester)
{
    bool firstError = true;
    bool secondError = true;
    for (int64_t i = 0; i < 10; i++)
    {
        secondError = true;
        if (!firstError)
        {
            break;
        }
        for (int64_t j = 0; j < 10; j++)
        {
            tester(i, j, firstError, secondError);
            if (!secondError)
            {
                break;
            }
        }
    }
}

void OnlinePartitionReopenTest::TestReopen()
{
    DoTest(std::bind(&OnlinePartitionReopenTest::InnerTestReopen, this, _1, _2, _3, _4));
    DoTest(std::bind(&OnlinePartitionReopenTest::InnerTestKVReopen, this, _1, _2, _3, _4));    
}

void OnlinePartitionReopenTest::TestForceReopen()
{
    DoTest(std::bind(&OnlinePartitionReopenTest::InnerTestForceReopen, this, _1, _2, _3, _4));
    DoTest(std::bind(&OnlinePartitionReopenTest::InnerTestKVForceReopen, this, _1, _2, _3, _4));
}

void OnlinePartitionReopenTest::TestSimpleProcess()
{
    for (int64_t i = 0; i < 10; i++)
    {
        InnerTestRollBackWithoutRT(i);
    }
}

void OnlinePartitionReopenTest::InnerTestReopen(
        int64_t triggerErrorIdx, int64_t continueFailIdx,
        bool& firstErrorTriggered, bool& secondErrorTriggered)
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        IndexPartitionOptions options;
        options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
        FakeMemoryQuotaController* fakeMemController = 
            new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
        util::MemoryQuotaControllerPtr memController(fakeMemController);
        IndexPartitionResource partitionResource;
        partitionResource.memoryQuotaController = memController;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
        if (!psm.Transfer(BUILD_FULL, mFullDocs, "", ""))
        {
            //when not open don't care
            return;
        }
    
        ASSERT_TRUE(psm.Transfer(BUILD_RT, mRtDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, "", ""));
        if (psm.Transfer(PE_REOPEN, "", "", ""))
        {
            //normal reopen not care
            return;
        }
        //check can normal search
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=10,updatable_multi_long=10"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=11,updatable_multi_long=11"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=13,updatable_multi_long=13"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=14,updatable_multi_long=14"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        //test can normal build check rt doc search
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, mRtDocs1, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=30,updatable_multi_long=30"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=31,updatable_multi_long=31"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=32,updatable_multi_long=32"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        while(!psm.Transfer(PE_REOPEN, "", "", ""))
        {
            //reopen fail can normal search
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=30,updatable_multi_long=30"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=31,updatable_multi_long=31"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=32,updatable_multi_long=32"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        }
        //normal reopen success
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=20,updatable_multi_long=20"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=21,updatable_multi_long=21"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=22,updatable_multi_long=22"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
        secondErrorTriggered = fakeMemController->HasTriggeredFirstError();
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        IndexPartitionOptions options;
        options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
        FakeMemoryQuotaController* fakeMemController = 
            new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
        util::MemoryQuotaControllerPtr memController(fakeMemController);
        IndexPartitionResource partitionResource;
        partitionResource.memoryQuotaController = memController;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
        if (!psm.Transfer(BUILD_FULL, mFullDocs, "", ""))
        {
            //when not open don't care
            return;
        }
    
        ASSERT_TRUE(psm.Transfer(BUILD_RT, mRtDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, "", ""));
        if (psm.Transfer(PE_REOPEN, "", "", ""))
        {
            //normal reopen not care
            return;
        }
        //check can normal search
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=10,updatable_multi_long=10"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=11,updatable_multi_long=11"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=13,updatable_multi_long=13"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=14,updatable_multi_long=14"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        //test can normal build check rt doc search
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, mRtDocs1, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=30,updatable_multi_long=30"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=31,updatable_multi_long=31"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=32,updatable_multi_long=32"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        while(!psm.Transfer(PE_REOPEN, "", "", ""))
        {
            //reopen fail can normal search
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=30,updatable_multi_long=30"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=31,updatable_multi_long=31"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=32,updatable_multi_long=32"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        }
        //normal reopen success
        ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=20,updatable_multi_long=20"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=21,updatable_multi_long=21"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=32,updatable_multi_long=32"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""));
        firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
        secondErrorTriggered = fakeMemController->HasTriggeredFirstError();        
    }
}

void OnlinePartitionReopenTest::InnerTestForceReopen(
        int64_t triggerErrorIdx, int64_t continueFailIdx,
        bool& firstErrorTriggered, bool& secondErrorTriggered)
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        IndexPartitionOptions options;
        options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
        options.GetOnlineConfig().enableForceOpen = false;
        FakeMemoryQuotaController* fakeMemController = 
            new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
        util::MemoryQuotaControllerPtr memController(fakeMemController);
        IndexPartitionResource partitionResource;
        partitionResource.memoryQuotaController = memController;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
        if (!psm.Transfer(BUILD_FULL, mFullDocs, "", ""))
        {
            //when not open don't care
            return;
        }
        ASSERT_TRUE(psm.Transfer(BUILD_RT, mRtDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, "", ""));
        while(!psm.Transfer(PE_REOPEN_FORCE, "", "", ""));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=20,updatable_multi_long=20"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=21,updatable_multi_long=21"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=22,updatable_multi_long=22"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=13,updatable_multi_long=13"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=14,updatable_multi_long=14"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
        firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
        secondErrorTriggered = fakeMemController->HasTriggeredFirstError();
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        IndexPartitionOptions options;
        options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
        options.GetOnlineConfig().enableForceOpen = false;
        FakeMemoryQuotaController* fakeMemController = 
            new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
        util::MemoryQuotaControllerPtr memController(fakeMemController);
        IndexPartitionResource partitionResource;
        partitionResource.memoryQuotaController = memController;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
        if (!psm.Transfer(BUILD_FULL, mFullDocs, "", ""))
        {
            //when not open don't care
            return;
        }
        ASSERT_TRUE(psm.Transfer(BUILD_RT, mRtDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, "", ""));
        while(!psm.Transfer(PE_REOPEN_FORCE, "", "", ""));
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=20,updatable_multi_long=20"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=21,updatable_multi_long=21"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:2", ""))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=13,updatable_multi_long=13"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=14,updatable_multi_long=14"))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
        EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:5", ""))
            << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
        firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
        secondErrorTriggered = fakeMemController->HasTriggeredFirstError();
    }    
}

void OnlinePartitionReopenTest::InnerTestRollBackWithoutRT(int64_t triggerErrorIdx)
{
    TearDown();
    SetUp();
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    FakeMemoryQuotaController* fakeMemController = 
        new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, 0);
    util::MemoryQuotaControllerPtr memController(fakeMemController);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    string fullDocs = "cmd=add,pk=0,long1=0,updatable_multi_long=0,ts=0";
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    if (!psm.Transfer(BUILD_FULL, fullDocs, "", ""))
    {
        //when not open don't care
        return;
    }

    string incDocs = "cmd=update_field,pk=0,long1=1,updatable_multi_long=1,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocs, "", ""));
    if (psm.Transfer(PE_REOPEN, "", "", ""))
    {
        //normal reopen not care
        return;
    }

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=0,updatable_multi_long=0"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "pk:0", "long1=1,updatable_multi_long=1"));
}

void OnlinePartitionReopenTest::TestAsyncDumpReopenFail()
{
    string queryStr = "pk:3;pk:0;pk:4;pk:1;pk:2";
    string resultStr = "long1=33;;long1=44;long1=4;long1=5";

    for (size_t i = 0; i < 10; i++)
    {
        {
            string incDoc = "cmd=add,pk=0,long1=0,updatable_multi_long=0,ts=0";
            InnerTestAsyncDumpReopen(i, incDoc, queryStr, resultStr);            
        }

        {
            string incDoc = "cmd=add,pk=1,long1=11,updatable_multi_long=11,ts=2";
            InnerTestAsyncDumpReopen(i, incDoc, queryStr, resultStr);            
        }

        {
            string incDoc = "cmd=add,pk=1,long1=11,updatable_multi_long=11,ts=3";
            InnerTestAsyncDumpReopen(i, incDoc, queryStr, resultStr);            
        }

        {
            string incDoc = "cmd=add,pk=1,long1=11,updatable_multi_long=11,ts=8";
            InnerTestAsyncDumpReopen(i, incDoc, queryStr, resultStr);            
        }
    }
}

void OnlinePartitionReopenTest::TestAsyncDumpReopenSuccess()
{
    {
        string incDoc = "cmd=add,pk=0,long1=0,updatable_multi_long=0,ts=0";
        string queryStr = "pk:3;pk:0;pk:4;pk:1;pk:2";
        string resultStr = "long1=33;;long1=44;long1=4;long1=5";
        InnerTestAsyncDumpReopen(NO_ERROR, incDoc, queryStr, resultStr);            
    }

    {
        string queryStr = "pk:3;pk:0;pk:4;pk:1;pk:2";
        string resultStr = "long1=33;;long1=44;long1=4;long1=5";
        string incDoc = "cmd=add,pk=1,long1=11,updatable_multi_long=11,ts=3";
        InnerTestAsyncDumpReopen(NO_ERROR, incDoc, queryStr, resultStr); 
    }

    {
        string queryStr = "pk:3;pk:0;pk:4;pk:1;pk:2";
        string resultStr = ";;long1=44;long1=4;long1=5";
        string incDoc = "cmd=add,pk=1,long1=11,updatable_multi_long=11,ts=4";
        InnerTestAsyncDumpReopen(NO_ERROR, incDoc, queryStr, resultStr); 
    }

    {
        string queryStr = "pk:3;pk:0;pk:4;pk:1;pk:2";
        string resultStr = ";long1=0;;long1=11;";
        string incDoc = "cmd=add,pk=1,long1=11,updatable_multi_long=11,ts=9";
        InnerTestAsyncDumpReopen(NO_ERROR, incDoc, queryStr, resultStr);            
    }
}

//reopenQuery: query1;query2;.... reopenResult:result1;result2
void OnlinePartitionReopenTest::InnerTestAsyncDumpReopen(size_t triggerErrorIdx, const string& incDocs,
                                                         const string& reopenQuery, const string& reopenResult)    
{
    TearDown();
    SetUp();
    DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000, false, true));
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    FakeMemoryQuotaController* fakeMemController = 
        new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, 0);
    util::MemoryQuotaControllerPtr memController(fakeMemController);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource, dumpContainer);
    string fullDocs = "cmd=add,pk=0,long1=0,updatable_multi_long=0,ts=0";
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));

    if (!psm.Transfer(BUILD_FULL, fullDocs, "", ""))
    {
        //when not open don't care
        return;
    }

    string rtDocs = "cmd=add,pk=0,long1=1,updatable_multi_long=1,ts=1;"
        "cmd=add,pk=1,long1=2,updatable_multi_long=2,ts=2;"
        "cmd=add,pk=3,long1=33,updatable_multi_long=33,ts=3;";

    if (!psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "pk:0", "long1=1,updatable_multi_long=1"))
    {
        return;
    }

    rtDocs = "cmd=delete,pk=0,ts=4;"
        "cmd=add,pk=1,long1=3,updatable_multi_long=3,ts=5;"
        "cmd=add,pk=4,long1=44,updatable_multi_long=44,ts=6;";
    if (!psm.Transfer(BUILD_RT_SEGMENT,rtDocs, "pk:0", ""))
    {
        return;
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=3,updatable_multi_long=3"));

    rtDocs = "cmd=add,pk=1,long1=4,updatable_multi_long=4,ts=7;"
        "cmd=add,pk=2,long1=5,updatable_multi_long=5,ts=8;";
        
    if (!psm.Transfer(BUILD_RT, rtDocs, "pk:1", "long1=4,updatable_multi_long=4"))
    {
        return;
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=5,updatable_multi_long=5"));
    
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocs, "", ""));
    vector<string> querys = StringUtil::split(reopenQuery, ";", false);
    vector<string> results = StringUtil::split(reopenResult, ";", false);
    if (psm.Transfer(PE_REOPEN, "", "", ""))
    {
        if (triggerErrorIdx == NO_ERROR)
        {
            for (size_t i = 0; i < querys.size(); i++)
            {
                ASSERT_TRUE(psm.Transfer(QUERY, "", querys[i], results[i])) << querys[i] << " " << results[i];        
            }       
        }
        return;
    }

    for (size_t i = 0; i < querys.size(); i++)
    {
        ASSERT_TRUE(psm.Transfer(QUERY, "", querys[i], results[i])) << querys[i] << " " << results[i];        
    }
    return;
}

void OnlinePartitionReopenTest::InnerTestKVReopen(
        int64_t triggerErrorIdx, int64_t continueFailIdx,
        bool& firstErrorTriggered, bool& secondErrorTriggered)
{
    TearDown();
    SetUp();
    IndexPartitionOptions options;
    options.GetBuildConfig(true).buildTotalMemory = 22;
    options.GetBuildConfig(false).buildTotalMemory = 40;
    options.GetBuildConfig(false).levelTopology = topo_hash_mod;
    options.GetBuildConfig(false).levelNum = 2;
    options.GetBuildConfig(false).keepVersionCount = 100;

    string field = "key:int32;value:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");



    FakeMemoryQuotaController* fakeMemController =
        new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
    util::MemoryQuotaControllerPtr memController(fakeMemController);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    if (!psm.Transfer(BUILD_FULL, mKVFullDocs, "", ""))
    {
        //when not open don't care
        return;
    }

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, mKVRtDocs0, "", ""));
    // test inMemorySegment inherit
    ASSERT_TRUE(psm.Transfer(BUILD_RT, mKVRtDocs1, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mKVIncDocs, "", ""));
    if (psm.Transfer(PE_REOPEN, "", "", ""))
    {
        //normal reopen not care
        return;
    }
    //check can normal search
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "7", ""));

    //test can normal build check rt doc search
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, mKVRtDocs2, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "7", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "8", "value=8"));

    while(!psm.Transfer(PE_REOPEN, "", "", ""))
    {
        //reopen fail can normal search
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", "value=4"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "7", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "8", "value=8"));
    }
    //normal reopen success
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=13"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "4", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "7", "value=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "8", "value=8"));

    firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
    secondErrorTriggered = fakeMemController->HasTriggeredFirstError();
}

void OnlinePartitionReopenTest::InnerTestKVForceReopen(
        int64_t triggerErrorIdx, int64_t continueFailIdx,
        bool& firstErrorTriggered, bool& secondErrorTriggered)
{
    TearDown();
    SetUp();

    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true; 
    options.GetBuildConfig(true).buildTotalMemory = 22;
    options.GetBuildConfig(false).buildTotalMemory = 40;
    options.GetBuildConfig(false).levelTopology = topo_hash_mod;
    options.GetBuildConfig(false).levelNum = 2;
    options.GetBuildConfig(false).keepVersionCount = 100;

    string field = "key:int32;value:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");

    FakeMemoryQuotaController* fakeMemController =
        new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
    util::MemoryQuotaControllerPtr memController(fakeMemController);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    if (!psm.Transfer(BUILD_FULL, mKVFullDocs, "", ""))
    {
        //when not open don't care
        return;
    }
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, mKVRtDocs0, "", ""))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, mKVRtDocs1, "", ""))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mKVIncDocs, "", ""))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
    while(!psm.Transfer(PE_REOPEN_FORCE, "", "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    EXPECT_TRUE(psm.Transfer(QUERY, "", "3", "value=13"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    EXPECT_TRUE(psm.Transfer(QUERY, "", "4", ""))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    EXPECT_TRUE(psm.Transfer(QUERY, "", "6", "value=6"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    EXPECT_TRUE(psm.Transfer(QUERY, "", "7", "value=7"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
    secondErrorTriggered = fakeMemController->HasTriggeredFirstError();
}

void OnlinePartitionReopenTest::TestForceOpen()
{
    DoTest(std::bind(&OnlinePartitionReopenTest::InnerTestForceOpen, this, _1, _2, _3, _4));
}

void OnlinePartitionReopenTest::InnerTestForceOpen(
        int64_t triggerErrorIdx, int64_t continueFailIdx,
        bool& firstErrorTriggered, bool& secondErrorTriggered)
{
    TearDown();
    SetUp();
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetOnlineConfig().enableForceOpen = true;
    FakeMemoryQuotaController* fakeMemController = 
        new FakeMemoryQuotaController(1024*1024*1024, triggerErrorIdx, continueFailIdx);
    util::MemoryQuotaControllerPtr memController(fakeMemController);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    if (!psm.Transfer(BUILD_FULL, mFullDocs, "", ""))
    {
        //when not open don't care
        return;
    }
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, "", ""));
    while(!psm.Transfer(PE_REOPEN_FORCE, "", "", "")) {};
    ASSERT_TRUE(psm.Transfer(BUILD_RT, mRtDocs2, "", ""));
    
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:0", "long1=30,updatable_multi_long=30"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:1", "long1=31,updatable_multi_long=31"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:2", "long1=32,updatable_multi_long=32"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:3", "long1=33,updatable_multi_long=33"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:4", "long1=34,updatable_multi_long=34"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;    
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:5", "long1=5,updatable_multi_long=5"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl;
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:6", "long1=66,updatable_multi_long=66"))
        << "triggerErrorIdx : " << triggerErrorIdx << ", continueFailIdx: " << continueFailIdx << endl; 
    firstErrorTriggered = fakeMemController->HasTriggeredFirstError();
    secondErrorTriggered = fakeMemController->HasTriggeredFirstError();
}

void OnlinePartitionReopenTest::TestAsyncDumpForceOpen()
{
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    FakeMemoryQuotaController* fakeMemController =
        new FakeMemoryQuotaController(1024*1024*1024, 5, 0);
    // 5th tryAllocate failed to trigger force open
    util::MemoryQuotaControllerPtr memController(fakeMemController);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;

    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, partitionResource);
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, mFullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, mRtDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN_FORCE, "", "", ""));
    ASSERT_TRUE(fakeMemController->HasTriggeredFirstError());
}

IE_NAMESPACE_END(partition);

