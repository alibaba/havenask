#include "indexlib/partition/test/partition_counter_intetest.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionCounterTest);

PartitionCounterTest::PartitionCounterTest()
{
}

PartitionCounterTest::~PartitionCounterTest()
{
}

void PartitionCounterTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;long2:uint32;long3:uint32;long4:uint32;long5:uint32;long6:uint32;"
                   "multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;long2;long3;long4;multi_long;updatable_multi_long;packAttr1:long5,long6";
    string summary = "string1";

    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    mOptions = IndexPartitionOptions();
}

void PartitionCounterTest::CaseTearDown()
{
}

void PartitionCounterTest::TestUpdateCounters()
{
    PartitionStateMachine psm;
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 10;

    string field = "pk:string:pk;long1:uint32;";
    string index = "pk:primarykey64:pk;";
    string attr = "long1;";

    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    auto checkOfflineCounters = [this](int64_t addVal, int64_t updateVal, int64_t delVal) {
        Version version;
        VersionLoader::GetVersion(this->mRootDir, version, INVALID_VERSION);
        segmentid_t segId = version[version.GetSegmentCount() - 1];
        string segPath = "segment_" + std::to_string(segId) + "_level_0";
        auto seg = this->GET_PARTITION_DIRECTORY()->GetDirectory(segPath, true);
        ASSERT_TRUE(seg);
        ASSERT_TRUE(seg->IsExist(COUNTER_FILE_NAME));
        string counterJsonString;
        seg->Load(COUNTER_FILE_NAME, counterJsonString);
        util::CounterMap counterMap;
        counterMap.FromJsonString(counterJsonString);

        EXPECT_EQ(addVal, counterMap.GetAccCounter("offline.build.addDocCount")->Get());
        EXPECT_EQ(updateVal, counterMap.GetAccCounter("offline.build.updateDocCount")->Get());
        EXPECT_EQ(delVal, counterMap.GetAccCounter("offline.build.deleteDocCount")->Get());
    };

    string fullDocString = "cmd=add,pk=1,long1=1,ts=1;"
                           "cmd=add,pk=2,long1=2,ts=1;"
                           "cmd=add,pk=3,long1=3,ts=1;"
                           "cmd=update_field,pk=1,long1=12,ts=1;"
                           "cmd=delete,pk=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    checkOfflineCounters(3, 1, 1);

    string incDocString = "cmd=update_field,pk=2,long1=22,ts=1;"
                          "cmd=add,pk=3,long1=33,ts=1;"
                          "cmd=add,pk=9,long1=2,ts=1;"
                          "cmd=delete,pk=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));
    checkOfflineCounters(4, 2, 2);

    string rtDocString = "cmd=add,pk=10,long1=100,ts=2;"
                         "cmd=add,pk=30,long1=33,ts=2;"
                         "cmd=update_field,pk=9,long1=22,ts=2;"
                         "cmd=delete,pk=10,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    checkOfflineCounters(4, 2, 2);
    const auto& counterMap = psm.GetIndexPartition()->GetCounterMap();
    EXPECT_EQ(2, counterMap->GetAccCounter("online.build.addDocCount")->Get());
    EXPECT_EQ(1, counterMap->GetAccCounter("online.build.updateDocCount")->Get());
    EXPECT_EQ(1, counterMap->GetAccCounter("online.build.deleteDocCount")->Get());    
}

void PartitionCounterTest::TestOpenPartition()
{
    {  // prepare full index
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        // prepare empty partition
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));

        {
            // test online partition open empty partition
            IndexPartitionOptions options = mOptions;
            options.SetIsOnline(true);
            OnlinePartition onlinePartition;
            ASSERT_EQ(IndexPartition::OS_OK, onlinePartition.Open(mRootDir, "", mSchema, options));
        }

        // test offline partition open emtpy partition
        string fullDocStr = "cmd=add,pk=1,string1=hello,ts=1;" 
                            "cmd=add,pk=2,string1=hello,ts=1;" 
                            "cmd=add,pk=3,string1=hello,ts=1;" 
                            "cmd=update_field,pk=1,long1=1,ts=1;" 
                            "cmd=update_field,pk=2,long2=2,ts=1;"
                            "cmd=delete,pk=3,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));
        DirectoryPtr seg0 = GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", true);
        ASSERT_TRUE(seg0);
        CheckCounterValues(0, "offline.build", "addDocCount:3,updateDocCount:2,deleteDocCount:1");
        
        // test after reopen new segment, counter will be accumulated
        string incDocStr = "cmd=add,pk=4,string1=hello,ts=1;" 
                            "cmd=add,pk=5,string1=hello,ts=1;" 
                            "cmd=update_field,pk=4,long1=1,ts=1;" 
                            "cmd=update_field,pk=5,long2=2,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr, "", ""));
        DirectoryPtr seg1 = GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_0", true);
        ASSERT_TRUE(seg1);
        CheckCounterValues(1, "offline.build", "addDocCount:5,updateDocCount:4,deleteDocCount:1");
    }
    {  // test online partition open
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(true);
        OnlinePartition onlinePartition;
        ASSERT_EQ(IndexPartition::OS_OK, onlinePartition.Open(mRootDir, "", mSchema, options));
        CheckCounterMap(onlinePartition.GetCounterMap(), "offline.build", "addDocCount:5,updateDocCount:4,deleteDocCount:1");
    }
    {  // test offline partition open
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        string incDocStr = "cmd=add,pk=5,string1=hello,ts=1;" 
                           "cmd=add,pk=6,string1=hello,ts=1;" 
                           "cmd=add,pk=7,string1=hello,ts=1;" 
                           "cmd=update_field,pk=1,long1=1,ts=1;" 
                           "cmd=update_field,pk=2,long2=2,ts=1;"
                           "cmd=delete,pk=5,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr, "", ""));
        DirectoryPtr seg2 = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_0", true);
        ASSERT_TRUE(seg2);
        CheckCounterValues(2, "offline.build", "addDocCount:8,updateDocCount:6,deleteDocCount:2");
    }
}

void PartitionCounterTest::TestOpenOfflinePartitionWithNoCounterFile()
{
    {  // prepare full index
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string fullDocStr = "cmd=add,pk=1,string1=hello,ts=1;" 
                            "cmd=add,pk=2,string1=hello,ts=1;" 
                            "cmd=add,pk=3,string1=hello,ts=1;" 
                            "cmd=update_field,pk=1,long1=1,ts=1;" 
                            "cmd=update_field,pk=2,long2=2,ts=1;"
                            "cmd=delete,pk=3,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));
        DirectoryPtr seg0 = GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", true);
        seg0->RemoveFile(COUNTER_FILE_NAME);
        seg0->RemoveFile(SEGMENT_FILE_LIST);
        seg0->RemoveFile(DEPLOY_INDEX_FILE_NAME);
        DeployIndexWrapper dpWrapper("", "");
        dpWrapper.DumpSegmentDeployIndex(seg0);        
    }
    {   // test rt IndexBuilder open
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(true);
        OnlinePartition onlinePartition;
        ASSERT_EQ(IndexPartition::OS_OK, onlinePartition.Open(mRootDir, "", mSchema, options));
        ASSERT_TRUE(onlinePartition.GetCounterMap());
    }
    {   // test inc build from old index (no counter file)
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string incDocStr = "cmd=add,pk=4,string1=hello,ts=1;" 
                            "cmd=add,pk=5,string1=hello,ts=1;" 
                            "cmd=update_field,pk=4,long1=1,ts=1;" 
                            "cmd=update_field,pk=5,long2=2,ts=1;"
                            "cmd=delete,pk=2,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr, "", ""));
        DirectoryPtr seg1 = GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_0", true);
        ASSERT_TRUE(seg1);
        CheckCounterValues(1, "offline.build", "addDocCount:2,updateDocCount:2,deleteDocCount:1");
    }
}

void PartitionCounterTest::OverWriteCounterFile(
        segmentid_t segId, int64_t val1, int64_t val2, bool removeCounterFile)
{
    string segName = "segment_" + StringUtil::toString(segId) + "_level_0";
    DirectoryPtr segDir = GET_PARTITION_DIRECTORY()->GetDirectory(segName, true);
    ASSERT_TRUE(segDir->IsExist(COUNTER_FILE_NAME));
    segDir->RemoveFile(COUNTER_FILE_NAME);
    if (!removeCounterFile)
    {
        util::CounterMap offlineCounterMap;
        offlineCounterMap.GetStateCounter("offline.test1")->Set(val1); 
        offlineCounterMap.GetAccCounter("offline.test2")->Increase(val2); 
        segDir->Store(COUNTER_FILE_NAME, offlineCounterMap.ToJsonString());
    }
    segDir->RemoveFile(SEGMENT_FILE_LIST);
    segDir->RemoveFile(DEPLOY_INDEX_FILE_NAME);
    DeployIndexWrapper dpWrapper("", "");
    dpWrapper.DumpSegmentDeployIndex(segDir);
}

void PartitionCounterTest::TestReOpenPartition()
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "cmd=add,pk=1,string1=hello,ts=1", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pk=2,string1=hello2,ts=2", "", ""));
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    const util::CounterMapPtr& counterMap = indexPart->GetCounterMap();
    counterMap->GetAccCounter("online.test1")->Increase(10);
    counterMap->GetStateCounter("online.test2")->Set(20); 
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, "cmd=add,pk=3,string1=hello3,ts=3", "", ""));

    OverWriteCounterFile(1, 100, 111);
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    EXPECT_EQ(10, counterMap->GetAccCounter("online.test1")->Get());
    EXPECT_EQ(20, counterMap->GetStateCounter("online.test2")->Get());
    EXPECT_EQ(100, counterMap->GetStateCounter("offline.test1")->Get());
    EXPECT_EQ(111, counterMap->GetAccCounter("offline.test2")->Get());
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, "cmd=add,pk=4,string1=hello4,ts=4", "", ""));
    OverWriteCounterFile(2, 200, 211);
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    EXPECT_EQ(10, counterMap->GetAccCounter("online.test1")->Get());
    EXPECT_EQ(20, counterMap->GetStateCounter("online.test2")->Get());
    EXPECT_EQ(200, counterMap->GetStateCounter("offline.test1")->Get());
    EXPECT_EQ(211, counterMap->GetAccCounter("offline.test2")->Get());
    
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, "cmd=add,pk=5,string1=hello5,ts=4", "", ""));
    OverWriteCounterFile(3, 300, 311);
    ASSERT_TRUE(psm.Transfer(PE_REOPEN_FORCE, "", "", ""));
    EXPECT_EQ(10, counterMap->GetAccCounter("online.test1")->Get());
    EXPECT_EQ(20, counterMap->GetStateCounter("online.test2")->Get());
    EXPECT_EQ(300, counterMap->GetStateCounter("offline.test1")->Get());
    EXPECT_EQ(311, counterMap->GetAccCounter("offline.test2")->Get());    
}

void PartitionCounterTest::TestSinlgePartitionCounterMerge()
{
    PartitionStateMachine psm;
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 10;    
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "cmd=add,pk=1,string1=hello,ts=1", "", ""));
    OverWriteCounterFile(0, 10, 20);
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,pk=2,string2=hello,ts=1", "", ""));
    OverWriteCounterFile(1, 11, 21);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,pk=3,string2=hello,ts=1", "", ""));

    DirectoryPtr mergeSegDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_0", true);
    ASSERT_TRUE(mergeSegDir);
    util::CounterMap mergedCounterMap;
    string counterMapContent;
    mergeSegDir->Load(COUNTER_FILE_NAME, counterMapContent);
    mergedCounterMap.FromJsonString(counterMapContent);
    EXPECT_EQ(11, mergedCounterMap.GetStateCounter("offline.test1")->Get());
    EXPECT_EQ(21, mergedCounterMap.GetAccCounter("offline.test2")->Get());    
}

void PartitionCounterTest::TestPartitionMergeWithNoCounterFile()
{
    {
        PartitionStateMachine psm;
        mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
        mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 10;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;"
                          "cmd=add,pk=2,string1=hello,ts=1;"
                          "cmd=add,pk=3,string1=hello,ts=1;"
                          "cmd=add,pk=4,string1=hello,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
        OverWriteCounterFile(0, 0, 0, true);
        
        string incDocs = "cmd=add,pk=6,string1=hello,ts=1;"
                         "cmd=add,pk=5,string1=hello,ts=1;"
                         "cmd=add,pk=8,string1=hello,ts=1;"
                         "cmd=add,pk=9,string1=hello,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
        OverWriteCounterFile(1, 0, 0, true); 
    }
    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mRootDir, version));
    segDir->Init(false, true);
    mOptions.SetIsOnline(false);
    IndexPartitionMerger merger(segDir, mSchema, mOptions,
                                merger::DumpStrategyPtr(), NULL,
                                plugin::PluginManagerPtr());
    ASSERT_NO_THROW(merger.Merge(true));
    // check empty counter file exist
    string segName = "segment_2_level_0";
    DirectoryPtr segmentDir = GET_PARTITION_DIRECTORY()->GetDirectory(segName, true);
    ASSERT_TRUE(segmentDir->IsExist(COUNTER_FILE_NAME));
    string counterFileContent;
    segmentDir->Load(COUNTER_FILE_NAME, counterFileContent);
    EXPECT_EQ(util::CounterMap::EMPTY_COUNTER_MAP_JSON, counterFileContent);
}

// expectvalues : "long1:2,long2:2,long3:2,long4:1,updatable_multi_long:0"
void PartitionCounterTest::CheckCounterMap(const util::CounterMapPtr& counterMap, const string& prefix,
        const string& expectValues)
{
    ASSERT_TRUE(counterMap);
    vector<vector<string>> counterInfos;
    StringUtil::fromString(expectValues, counterInfos, ":", ",");

    for (auto& counterInfo : counterInfos)
    {
        ASSERT_EQ(2u, counterInfo.size());
        int64_t expectValue = StringUtil::fromString<int64_t>(counterInfo[1]);
        string counterPath = prefix + "." + counterInfo[0];
        EXPECT_EQ(expectValue, counterMap->GetAccCounter(counterPath)->Get()) << "failed counter:" << counterPath;
    }    
}
    
void PartitionCounterTest::CheckCounterValues(segmentid_t segId, const string& prefix,
        const string& expectValues)
{
    string segName = "segment_" + StringUtil::toString(segId) + "_level_0";
    DirectoryPtr segDir = GET_PARTITION_DIRECTORY()->GetDirectory(segName, true);
    ASSERT_TRUE(segDir->IsExist(COUNTER_FILE_NAME));
    string counterFileContent;
    segDir->Load(COUNTER_FILE_NAME, counterFileContent);

    util::CounterMapPtr counterMap(new util::CounterMap());
    counterMap->FromJsonString(counterFileContent);
    CheckCounterMap(counterMap, prefix, expectValues);
}

void PartitionCounterTest::TestUpdateFieldCounters()
{
    PartitionStateMachine psm;
    mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 10;    
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;"
                      "cmd=add,pk=2,string1=hello,ts=1;"
                      "cmd=add,pk=3,string1=hello,ts=1;"
                      "cmd=add,pk=4,string1=hello,ts=1;"
                      "cmd=update_field,pk=1,long1=11,long2=12,ts=1;"
                      "cmd=update_field,pk=2,long1=21,long2=22,long3=23,long5=1,long6=2,ts=1;"
                      "cmd=update_field,pk=4,long3=43,long4=44,long6=3,ts=1;";                      

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    CheckCounterValues(1, "offline.updateField",
                       "long1:2,long2:2,long3:2,long4:1,long5:1,long6:2,updatable_multi_long:0");

    string incDocs = "cmd=update_field,pk=2,long1=11,long2=12,ts=1;" 
                     "cmd=update_field,pk=3,long1=11,long2=22,long3=23,long5=3,ts=1;" 
                     "cmd=update_field,pk=2,long3=43,long4=44,long6=88,ts=1;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
    CheckCounterValues(3, "offline.updateField",
                       "long1:4,long2:4,long3:4,long4:2,long5:2,long6:3,updatable_multi_long:0");

    string rtDocs = "cmd=update_field,pk=2,long1=91,long2=92,ts=2;" 
                    "cmd=update_field,pk=3,long1=91,long2=92,long3=93,long5=93,ts=2;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=update_field,pk=2,long3=93,long4=94,long6=98,ts=2;", "", ""));

    auto indexPartition = psm.GetIndexPartition();
    auto counterMap = indexPartition->GetCounterMap();
    auto onlineUpdateCountes = counterMap->FindCounters("online.updateField");
    ASSERT_TRUE(onlineUpdateCountes.empty());
}

void PartitionCounterTest::TestMergeTimeCounters()
{
    {
        PartitionStateMachine psm;
        mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
        mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 10;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string fullDocs = "cmd=add,pk=1,string1=hello,ts=1;"
                          "cmd=add,pk=2,string1=hello,ts=1;"
                          "cmd=add,pk=3,string1=hello,ts=1;"
                          "cmd=add,pk=4,string1=hello,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));

        string incDocs = "cmd=add,pk=1,string1=hello,ts=1;"
                         "cmd=add,pk=5,string1=hello,ts=1;"
                         "cmd=add,pk=8,string1=hello,ts=1;"
                         "cmd=add,pk=9,string1=hello,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    }
    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mRootDir, version));
    segDir->Init(false, true);
    mOptions.SetIsOnline(false);
    IndexPartitionMerger merger(segDir, mSchema, mOptions,
                                merger::DumpStrategyPtr(),
                                NULL, plugin::PluginManagerPtr());
    auto counterMap = merger.GetCounterMap();
    merger.Merge(true);
    cout << counterMap->ToJsonString() << endl;
}

void PartitionCounterTest::TestOfflineRecover()
{
    IndexPartitionOptions options = mOptions;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetOfflineConfig().enableRecoverIndex = true;
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();    
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        // create version.0 : s0
        string seg0Data = "cmd=add,pk=1,string1=hello,ts=1;" 
                          "cmd=add,pk=2,string1=hello,ts=1;" 
                          "cmd=add,pk=3,string1=hello,ts=1;" 
                          "cmd=update_field,pk=1,long1=1,ts=1;" 
                          "cmd=update_field,pk=2,long2=2,ts=1;"
                          "cmd=delete,pk=3,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, seg0Data, "", ""));
        ASSERT_TRUE(partDir->IsExist("version.0"));

        // create version.1 : s1, s0
        string seg1Data = "cmd=add,pk=4,string1=hello,ts=1;" 
                          "cmd=add,pk=5,string1=hello,ts=1;" 
                          "cmd=add,pk=6,string1=hello,ts=1;" 
                          "cmd=update_field,pk=1,long1=1,ts=1;" 
                          "cmd=update_field,pk=2,long2=2,ts=1;"
                          "cmd=delete,pk=4,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg1Data, "", ""));
        ASSERT_TRUE(partDir->IsExist("version.1"));
        ASSERT_TRUE(partDir->IsExist("version.0")); 

        // create version.2 : s2, s1, s0
        string seg2Data = "cmd=add,pk=7,string1=hello,ts=1;" 
                          "cmd=add,pk=8,string1=hello,ts=1;" 
                          "cmd=add,pk=9,string1=hello,ts=1;" 
                          "cmd=update_field,pk=1,long1=1,ts=1;" 
                          "cmd=update_field,pk=2,long2=2,ts=1;"
                          "cmd=delete,pk=7,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg2Data, "", ""));
    
        ASSERT_TRUE(partDir->IsExist("version.2"));
        ASSERT_TRUE(partDir->IsExist("version.1"));
        ASSERT_TRUE(partDir->IsExist("version.0"));     
        // remove version.2, but keep segments for recovery
        partDir->RemoveFile("version.2");
    }
    {
        // create version.2 : s3, s2, s1, s0
        // check after recovery, counter is correct
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        string seg3Data = "cmd=add,pk=10,string1=hello,ts=1;" 
                          "cmd=add,pk=11,string1=hello,ts=1;" 
                          "cmd=add,pk=12,string1=hello,ts=1;" 
                          "cmd=update_field,pk=1,long1=1,ts=1;" 
                          "cmd=update_field,pk=2,long2=2,ts=1;"
                          "cmd=delete,pk=10,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg3Data, "", ""));
        ASSERT_TRUE(partDir->IsExist("version.2"));
        CheckCounterValues(3, "offline.build", "addDocCount:12,updateDocCount:8,deleteDocCount:4");
    }
    {   // check counters in invalid segments( with no segmentInfo) will not be recovered
        partDir->RemoveFile("version.2");
        DirectoryPtr seg3 = partDir->GetDirectory("segment_3_level_0", true);
        ASSERT_TRUE(seg3->IsExist(COUNTER_FILE_NAME));
        seg3->RemoveFile(SEGMENT_INFO_FILE_NAME);

        // TODO: check this 
        IE_NAMESPACE(config)::LoadConfigList loadConfigList;
        RESET_FILE_SYSTEM(loadConfigList, false);
        
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        string newSegData = "cmd=add,pk=11,string1=hello,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, newSegData, "", ""));
        ASSERT_TRUE(partDir->IsExist("version.2"));
        CheckCounterValues(3, "offline.build", "addDocCount:10,updateDocCount:6,deleteDocCount:3");
    }
}

void PartitionCounterTest::MakeOneByteFile(
        const string& dir, const string& fileName)
{
    string filePath = FileSystemWrapper::JoinPath(dir, fileName);
    FileSystemWrapper::AtomicStore(filePath, "0");
}

void PartitionCounterTest::PrepareSegmentData(
        const IndexPartitionSchemaPtr& schema, const string& segmentDir,
        int64_t docCount)
{
    string indexDir = FileSystemWrapper::JoinPath(segmentDir, INDEX_DIR_NAME);
    auto indexSchema = schema->GetIndexSchema();
    for (auto iter = indexSchema->Begin(); iter != indexSchema->End(); ++iter)
    {
        auto indexName = (*iter)->GetIndexName();
        auto indexType = (*iter)->GetIndexType();

        if (indexType == it_primarykey64) // 128/trie
        {
            string singleIndexDir = FileSystemWrapper::JoinPath(indexDir, indexName);            
            MakeOneByteFile(singleIndexDir, PRIMARY_KEY_DATA_FILE_NAME);
            auto pkIndexConfig = DYNAMIC_POINTER_CAST(SingleFieldIndexConfig, *iter);
            string pkAttrDir = FileSystemWrapper::JoinPath(singleIndexDir,
                    string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + "_" +
                    pkIndexConfig->GetFieldConfig()->GetFieldName());
            MakeOneByteFile(pkAttrDir, PRIMARY_KEY_DATA_FILE_NAME);
        }
        else
        {
            string singleIndexDir = FileSystemWrapper::JoinPath(indexDir, indexName);
            MakeOneByteFile(singleIndexDir, POSTING_FILE_NAME);
            MakeOneByteFile(singleIndexDir, DICTIONARY_FILE_NAME);
            MakeOneByteFile(singleIndexDir, BITMAP_DICTIONARY_FILE_NAME);
            MakeOneByteFile(singleIndexDir, BITMAP_POSTING_FILE_NAME);
            if (indexType == it_pack || indexType == it_expack)
            {
                string sectionDir = FileSystemWrapper::JoinPath(indexDir, indexName + "_section");
                MakeOneByteFile(sectionDir, ATTRIBUTE_DATA_FILE_NAME);
                MakeOneByteFile(sectionDir, ATTRIBUTE_OFFSET_FILE_NAME);
            }
        }
    }
    string deletionmapDir = FileSystemWrapper::JoinPath(segmentDir, DELETION_MAP_DIR_NAME);
    FileSystemWrapper::MkDir(deletionmapDir);
    string attrDir = FileSystemWrapper::JoinPath(segmentDir, ATTRIBUTE_DIR_NAME);
    auto attrSchema = schema->GetAttributeSchema();
    for (auto iter = attrSchema->Begin(); iter != attrSchema->End(); ++iter)
    {
        string singleAttrDir = FileSystemWrapper::JoinPath(attrDir, (*iter)->GetAttrName());
        MakeOneByteFile(singleAttrDir, ATTRIBUTE_DATA_FILE_NAME);
        MakeOneByteFile(singleAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
        MakeOneByteFile(singleAttrDir, string("0_1.") + ATTRIBUTE_PATCH_FILE_NAME);
    }

    for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); ++i)
    {
        auto packAttrConfig = attrSchema->GetPackAttributeConfig(i);
        auto packName = packAttrConfig->GetAttrName();
        string singleAttrDir = FileSystemWrapper::JoinPath(attrDir, packName);
        MakeOneByteFile(singleAttrDir, ATTRIBUTE_DATA_FILE_NAME);
        MakeOneByteFile(singleAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
        MakeOneByteFile(singleAttrDir, string("0_1.") + ATTRIBUTE_PATCH_FILE_NAME);        
    }

    if (schema->NeedStoreSummary())
    {
        string summaryDir = FileSystemWrapper::JoinPath(segmentDir, SUMMARY_DIR_NAME);
        MakeOneByteFile(summaryDir, SUMMARY_DATA_FILE_NAME);
        MakeOneByteFile(summaryDir, SUMMARY_OFFSET_FILE_NAME);
    }

    auto counterPath = FileSystemWrapper::JoinPath(segmentDir, COUNTER_FILE_NAME);
    FileSystemWrapper::AtomicStore(counterPath, util::CounterMap::EMPTY_COUNTER_MAP_JSON);

    string segInfoPath = FileSystemWrapper::JoinPath(segmentDir, SEGMENT_INFO_FILE_NAME);
    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.Store(segInfoPath);
}

void PartitionCounterTest::TestPartitionDocCounter()
{
    PrepareSegmentData(mSchema, FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0"), 100000000000);
    PrepareSegmentData(mSchema, FileSystemWrapper::JoinPath(mRootDir, "segment_1_level_0"), 1);
    Version version(0);
    version.AddSegment(0);
    version.AddSegment(1);
    version.Store(mRootDir, false);
    mOptions = IndexPartitionOptions();
    mOptions.SetIsOnline(false);
    util::CounterMapPtr counterMap(new util::CounterMap());
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto buildingPartData = PartitionDataCreator::CreateBuildingPartitionData(
            GET_FILE_SYSTEM(), mSchema, mOptions,
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
            dumpContainer, version, NULL, "", InMemorySegmentPtr(), counterMap);
    buildingPartData->CreateNewSegment();
    buildingPartData->UpdatePartitionInfo();
    string nodePath = "offline.partitionDocCount";
    EXPECT_EQ(100000000001, counterMap->GetStateCounter(nodePath)->Get());
}

void PartitionCounterTest::TestSizeCounters()
{
    util::CounterMapPtr counterMap(new util::CounterMap());

    PrepareSegmentData(mSchema, FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0"));
    PrepareSegmentData(mSchema, FileSystemWrapper::JoinPath(mRootDir, "segment_1_level_0"));

    auto subSchema = SchemaMaker::MakeSchema(
            "sub_string:string;sub_long:uint32;string1:string",
            "sub_pk:primarykey64:sub_string;",
            "sub_long",
            "string1");
    PrepareSegmentData(subSchema, FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0/sub_segment"));
    PrepareSegmentData(subSchema, FileSystemWrapper::JoinPath(mRootDir, "segment_1_level_0/sub_segment"));
    mSchema->SetSubIndexPartitionSchema(subSchema);

    mOptions = IndexPartitionOptions();
    Version version(0);
    version.AddSegment(0);
    version.AddSegment(1);
    version.Store(mRootDir, false);
    mOptions.SetIsOnline(true);

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto buildingPartData = PartitionDataCreator::CreateBuildingPartitionData(
        GET_FILE_SYSTEM(), mSchema, mOptions,
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        dumpContainer, version, NULL, "", InMemorySegmentPtr(), counterMap);

    std::map<string, int64_t> expectedSize = {
        {"attribute.long1", 4},
        {"attribute.long1(patch)", 2},
        {"attribute.long2", 4},
        {"attribute.long2(patch)", 2},
        {"attribute.long2", 4},
        {"attribute.long2(patch)", 2},
        {"attribute.long3", 4},
        {"attribute.long3(patch)", 2},
        {"attribute.long4", 4},
        {"attribute.long4(patch)", 2},
        {"attribute.multi_long", 4},
        {"attribute.multi_long(patch)", 2},
        {"attribute.packAttr1", 4},
        {"attribute.packAttr1(patch)", 2},
        {"attribute.updatable_multi_long", 4},
        {"attribute.updatable_multi_long(patch)", 2},
        {"inverted_index.index1", 8},
        {"inverted_index.pack1", 12},
        {"inverted_index.pk", 4},
        {"summary", 4},
        {"attribute.sub_long", 4},
        {"attribute.sub_long(patch)", 2},
        {"inverted_index.sub_pk", 4},
        {"sub_summary", 4},
    };

    CheckSizeCounters(counterMap, expectedSize);
}

void PartitionCounterTest::CheckSizeCounters(const util::CounterMapPtr& counterMap,
                       const std::map<string, int64_t>& expectedSize)
{
    for (const auto& kv : expectedSize)
    {
        string nodePath = "offline.indexSize." + kv.first;
        EXPECT_EQ(kv.second, counterMap->GetStateCounter(nodePath)->Get()) << kv.first;
    }
}

void PartitionCounterTest::TestSizeCounterSkipOnlineSegments()
{
    PartitionStateMachine psm;
        mOptions.GetOnlineConfig().onlineKeepVersionCount = 10;
        mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 10;    
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string fullDocString = "cmd=add,pk=1,long1=1,ts=1;"
                           "cmd=add,pk=2,long1=2,ts=1;"
                           "cmd=add,pk=3,long1=3,ts=1;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    auto counterMap = psm.GetIndexPartition()->GetCounterMap();
    int64_t long1Size = counterMap->GetStateCounter("offline.indexSize.attribute.long1")->Get();
    EXPECT_GT(long1Size, 0);

    string rtDocString = "cmd=add,pk=8,long1=33,ts=1;"
                         "cmd=add,pk=9,long1=2,ts=1;"
                         "cmd=add,pk=10,long1=2,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    string incDocString = "cmd=update_field,pk=10,long2=33,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));

    counterMap = psm.GetIndexPartition()->GetCounterMap();
    ASSERT_EQ(long1Size, counterMap->GetStateCounter("offline.indexSize.attribute.long1")->Get());
}

IE_NAMESPACE_END(partition);

