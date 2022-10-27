#include <autil/Thread.h>
#include <autil/StringUtil.h>
#include "indexlib/partition/test/partition_mem_control_perf_test_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/file_system/test/load_config_list_creator.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionMemControlPerfTest);

PartitionMemControlPerfTest::PartitionMemControlPerfTest()
{
}

PartitionMemControlPerfTest::~PartitionMemControlPerfTest()
{
}

void PartitionMemControlPerfTest::CaseSetUp()
{
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_ATTRIBUTE);
    //10 version
    size_t currentDoc = 0;
    for (size_t i = 0; i < 10; i++)
    {
        stringstream ss;
        for (size_t j = 1; j < 100000; j++)
        {
            ss << currentDoc << ",";
            currentDoc++;
        }
        ss << currentDoc;
        currentDoc++;
        provider.Build(ss.str(), SFP_OFFLINE);
    }
    mSchema = provider.GetSchema();
}

void PartitionMemControlPerfTest::CaseTearDown()
{
    mMemController.reset();
}

void PartitionMemControlPerfTest::TestSimpleProcess()
{
    //todo: case fail because writer open mem not control
    mStop = false;
    size_t partitionThreadNum = 3;
    vector<ThreadPtr> threads;
    mMemController.reset(new util::MemoryQuotaController(40*1024*1024));
    threads.push_back(Thread::createThread(tr1::bind(&PartitionMemControlPerfTest::CheckMemControlThread, this)));
    for (size_t i = 0; i < partitionThreadNum; i++)
    {
        threads.push_back(
                Thread::createThread(tr1::bind(&PartitionMemControlPerfTest::PartitionOpenReopenThread,
                                this, StringUtil::toString(i))));
    }
    sleep(10);
    mStop = true;
}

OnlinePartitionPtr PartitionMemControlPerfTest::CreateOnlinePartition(
        const string& partitionName)
{
    string rootPath = GET_TEST_DATA_PATH();
    OnlinePartitionPtr onlinePartition(new OnlinePartition(
                    partitionName, mMemController, NULL));
    IndexPartitionOptions options;
    options.GetOnlineConfig().loadConfigList.PushBack(
            LoadConfigListCreator::MakeMmapLoadConfig(true, true));
    IndexPartition::OpenStatus status = onlinePartition->Open(
            rootPath, "", mSchema, options, 0);
    if (status == IndexPartition::OS_OK)
    {
        cout << "partition:" << partitionName << " open ok!" << endl;
        return onlinePartition;
    }
    else
    {
        assert(status == IndexPartition::OS_LACK_OF_MEMORY);
        cout << "partition:" << partitionName << " open fail lack of mem!["
             << mMemController->GetFreeQuota() << "]" << endl;
        return OnlinePartitionPtr();
    }
}

void PartitionMemControlPerfTest::CheckMemControlThread()
{
    while(!mStop)
    {
        int64_t freeMem = mMemController->GetFreeQuota();
        if (freeMem < 0)
        {
            cout << "free mem less than zero:" << freeMem << endl;
        }
        ASSERT_TRUE(freeMem >= 0);
        usleep(1000);
    }
}

void PartitionMemControlPerfTest::PartitionOpenReopenThread(const string partitionName)
{
    OnlinePartitionPtr onlinePartition;
    string currentPartitionName = partitionName;
    versionid_t lastLoadedVersion = 0;
    while(!mStop)
    {
        if (!onlinePartition)
        {
            onlinePartition = CreateOnlinePartition(currentPartitionName);
            lastLoadedVersion = 0;
            usleep(10000);
            continue;
        }

        if (lastLoadedVersion == 9)
        {
            onlinePartition.reset();
            usleep(10000);
            continue;
        }
        versionid_t targetVersion = lastLoadedVersion + 1;
        IndexPartition::OpenStatus status = onlinePartition->ReOpen(false, targetVersion);
        if (status == IndexPartition::OS_OK)
        {
            cout << "partition:" << currentPartitionName << " reopen ok!" << endl;
            lastLoadedVersion = targetVersion;
        }
        else
        {
            status = onlinePartition->ReOpen(true, targetVersion);
            if (status == IndexPartition::OS_OK)
            {
                cout << "partition:" << currentPartitionName << " force reopen!" << endl;
            }
            else
            {
                cout << "partition:" << currentPartitionName << 
                    "free mem[" << mMemController->GetFreeQuota() <<
                    "] reopen fail lack of mem! release partition" << endl;
                onlinePartition.reset();                
            }
        }
        usleep(10000);
    }
}

IE_NAMESPACE_END(partition);

