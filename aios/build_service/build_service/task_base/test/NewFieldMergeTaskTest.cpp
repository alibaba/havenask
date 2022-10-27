#include "build_service/test/unittest.h"
#include "build_service/task_base/NewFieldMergeTask.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/task_base/test/FakeAlterFieldMerger.h"
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/partition/online_partition.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/partition/remote_access/partition_resource_provider_factory.h>
#include <indexlib/test/partition_state_machine.h>
#include "indexlib/testlib/indexlib_partition_creator.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::common;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);

namespace build_service {
namespace task_base {

class NewFieldMergeTaskTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
    void clearUselessFile();
    void innerTestRestart();

    void innerSimpleTest(bool incParallelBuild);
    void doAlterFieldMerge(
        const string& configPath, const string& root, const string& alignVersion);

private:
    std::string _indexPath;
    std::string _configPath;
    std::string _partitionPath;
    IndexPartitionSchemaPtr _schema;
    KeyValueMap _kvMap;
};

void NewFieldMergeTaskTest::setUp() {
    string originIndex = TEST_DATA_PATH"/original_index/";
    _indexPath = GET_TEST_DATA_PATH() + "/original_index";
    FileUtil::atomicCopy(originIndex, _indexPath);
    _configPath = TEST_DATA_PATH"/new_field_merge_task_config/";
    _partitionPath = _indexPath + "/simple/generation_5/partition_0_65535/";
    _kvMap[CONFIG_PATH] = _configPath;
    _kvMap[CLUSTER_NAMES] = "simple";
    _kvMap[GENERATION_ID] = "5";
    _kvMap[INDEX_ROOT_PATH] = _indexPath;
    _kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    _kvMap[MERGE_CONFIG_NAME] = "alter_field";
    _kvMap[MERGE_TIMESTAMP] = "1";
    _kvMap[MERGE_PARALLEL_NUM] = "2";
    _kvMap[ALIGNED_VERSION_ID] = "10";
    clearUselessFile();
}

void NewFieldMergeTaskTest::clearUselessFile() {
    vector<string> files = {"patch_index_1", "version.10", "version.11",
                            "version.12", "schema.json.1", "patch_meta.10",
                            "patch_meta.11", "patch_meta.12", "deploy_meta.10",
                            "deploy_meta.11", "deploy_meta.12", "alter_new_field",
                            "segment_1_level_0", "segment_2_level_0"};
    for (auto& f: files) {
        string dir = FileSystemWrapper::JoinPath(_partitionPath, f);
        if (FileSystemWrapper::IsExist(dir)) {
            FileSystemWrapper::DeleteFile(dir);
        }
    }
}

void NewFieldMergeTaskTest::tearDown() {
}

enum TestStep {
    ALTER_INIT,
    ALTER_BEGIN_MAP,
    ALTER_MERGE_ONE,
    ALTER_MERGE_TWO,
    ALTER_END_MAP
};

void NewFieldMergeTaskTest::innerTestRestart() {
    TestStep failStep = ALTER_INIT;
    string jobParam = legacy::ToJsonString(_kvMap);
    do{
        NewFieldMergeTask task(NULL);
        NewFieldMergeTask task2(NULL);
        ASSERT_TRUE(task.init(jobParam));
        ASSERT_TRUE(task2.init(jobParam));
        if (!task.run(0, TaskBase::MERGE_MAP)) {
            failStep = ALTER_BEGIN_MAP;
            break;
        }
        if (!task.run(0, TaskBase::MERGE_REDUCE)) {
            failStep = ALTER_MERGE_ONE;
        }
        if (!task2.run(1, TaskBase::MERGE_REDUCE)) {
            failStep = ALTER_MERGE_TWO;
        }
        if (failStep != ALTER_INIT) {
            break;
        }
        if (!task.run(0, TaskBase::END_MERGE_MAP)) {
            failStep = ALTER_END_MAP;
        }
    } while(0);
    if (failStep == ALTER_INIT) {
        return;
    }
    ASSERT_NE(ALTER_INIT, failStep);
    {
        NewFieldMergeTask task(NULL);
        NewFieldMergeTask task2(NULL);
        ASSERT_TRUE(task.init(jobParam));
        ASSERT_TRUE(task2.init(jobParam));
        if (failStep == ALTER_BEGIN_MAP) {
            ASSERT_TRUE(task.run(0, TaskBase::MERGE_MAP));
        }
        if (failStep == ALTER_BEGIN_MAP || failStep == ALTER_MERGE_ONE) {
            ASSERT_TRUE(task.run(0, TaskBase::MERGE_REDUCE));
        }
        if (failStep == ALTER_BEGIN_MAP || failStep == ALTER_MERGE_TWO) {
            ASSERT_TRUE(task2.run(1, TaskBase::MERGE_REDUCE));
        }
        ASSERT_TRUE(task.run(0, TaskBase::END_MERGE_MAP));
    }
    IndexPartitionOptions options;
    PartitionResourceProviderPtr provider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(_partitionPath, options, "");
    ASSERT_TRUE(provider);
    auto iter = provider->CreatePartitionIterator();
    auto attrIter = iter->CreateSingleAttributeIterator("patch_field2", 0);
    ASSERT_TRUE(attrIter->IsValid());
    ASSERT_EQ("0", attrIter->GetValueStr());
}

TEST_F(NewFieldMergeTaskTest, testRestart) {
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/custom_alter_field_config/";
    for (size_t i = 1; i <= 28; i ++) {
        FakeAlterFieldMerger::setErrorStep(i);
        FakeAlterFieldMerger::init();
        clearUselessFile();
        innerTestRestart();
    }
}

TEST_F(NewFieldMergeTaskTest, testSimple) {
    innerSimpleTest(false);
}

TEST_F(NewFieldMergeTaskTest, testIncParallelBuild) {
    innerSimpleTest(true);
}
    
TEST_F(NewFieldMergeTaskTest, testAlterFieldChangeSchemaIdOnly) {
    string root = GET_TEST_DATA_PATH();
    string indexPath = GET_TEST_DATA_PATH() + "/simple/generation_0/partition_0_65535";
    string field = "name:string:false:false:uniq;price:uint32;"
        "category:int32:true:true;tags:string:true";
    string index = "pk:primarykey64:name;index:number:price";
    string attributes = "name;price;category;tags";
    IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
        "index_table", field, index, attributes, "");
    string docs = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;"
        "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,ts=0;"
        "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,ts=0;";
    IndexPartitionOptions options;
    IndexlibPartitionCreator::CreateIndexPartition(
        schema, indexPath, docs, options, "", false);

    string configPath = TEST_DATA_PATH"/schema_change_config/";
    IndexPartitionSchemaPtr newSchema = schema;
    newSchema->SetSchemaVersionId(1);
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
    FileUtil::writeFile(configPath + "schemas/simple_schema.json", ToJsonString(newSchema));
    doAlterFieldMerge(configPath, root, "10");
    {
        //check index value
        IndexPartitionOptions options;
        PartitionResourceProviderPtr provider =
            PartitionResourceProviderFactory::GetInstance()->CreateProvider(indexPath, options, "");
        ASSERT_TRUE(provider);
        auto iter = provider->CreatePartitionIterator();
        auto attrIter = iter->CreateSingleAttributeIterator("price", 0);
        ASSERT_TRUE(attrIter);
        ASSERT_EQ(3u, attrIter->GetDocCount());
        vector<int64_t> prices;
        prices.push_back(10);
        prices.push_back(20);
        prices.push_back(30);
        for (size_t i = 0; i < 3; i++) {
            ASSERT_TRUE(attrIter->IsValid());
            ASSERT_EQ(StringUtil::toString(prices[i]), attrIter->GetValueStr());
            attrIter->MoveToNext();
        }
        ASSERT_FALSE(attrIter->IsValid());
    }
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
}

void NewFieldMergeTaskTest::doAlterFieldMerge(
    const std::string& configPath, const std::string& root, const std::string& alignVersion)
{
    string localConfigPath = PathDefine::getLocalConfigPath();
    FileUtil::removeIfExist(localConfigPath);
    KeyValueMap kvMap;
    kvMap[CONFIG_PATH] = configPath;
    kvMap[CLUSTER_NAMES] = "simple";
    kvMap[GENERATION_ID] = "0";
    kvMap[INDEX_ROOT_PATH] = root;
    kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    kvMap[MERGE_CONFIG_NAME] = "alter_field";
    kvMap[MERGE_TIMESTAMP] = "1";
    kvMap[MERGE_PARALLEL_NUM] = "1";
    kvMap[ALIGNED_VERSION_ID] = alignVersion;
    kvMap[WORKER_PATH_VERSION] = "0";
    NewFieldMergeTaskPtr task(new NewFieldMergeTask(NULL));
    string jobParam = legacy::ToJsonString(kvMap);

    ASSERT_TRUE(task->init(jobParam));
    ASSERT_TRUE(task->run(0, TaskBase::MERGE_MAP));
    ASSERT_TRUE(task->run(0, TaskBase::MERGE_REDUCE));
    ASSERT_TRUE(task->run(0, TaskBase::END_MERGE_MAP));    
}

TEST_F(NewFieldMergeTaskTest, testDeleteFields) {
    string root = GET_TEST_DATA_PATH();
    string indexPath = GET_TEST_DATA_PATH() + "/simple/generation_0/partition_0_65535";
    string field = "name:string:false:false:uniq;price:uint32;"
        "category:int32:true:true;tags:string:true";
    string index = "pk:primarykey64:name;index:number:price";
    string attributes = "name;price;category;tags";
    IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
        "index_table", field, index, attributes, "");
    string docs = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;"
        "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,ts=0;"
        "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,ts=0;";
    IndexPartitionOptions options;
    IndexlibPartitionCreator::CreateIndexPartition(
        schema, indexPath, docs, options, "", false);
    string configPath = TEST_DATA_PATH"/schema_change_config/";
    string newIndex = "pk:primarykey64:name";
    string newAttrs = "name;category;tags";
    IndexPartitionSchemaPtr newSchema = IndexlibPartitionCreator::CreateSchema(
        "index_table", field, newIndex, newAttrs, "");
    newSchema->SetSchemaVersionId(1);
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
    FileUtil::writeFile(configPath + "schemas/simple_schema.json", ToJsonString(newSchema));
    doAlterFieldMerge(configPath, root, "10");
    {
        //check price attribute not exit
        IndexPartitionOptions options;
        PartitionResourceProviderPtr provider =
            PartitionResourceProviderFactory::GetInstance()->CreateProvider(indexPath, options, "");
        ASSERT_TRUE(provider);
        auto iter = provider->CreatePartitionIterator();
        auto attrIter = iter->CreateSingleAttributeIterator("price", 0);
        ASSERT_FALSE(attrIter);
    }

    newIndex = "pk:primarykey64:name";
    newAttrs = "name;category;tags;price";
    newSchema = IndexlibPartitionCreator::CreateSchema(
        "index_table", field, newIndex, newAttrs, "");
    newSchema->SetSchemaVersionId(2);
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
    FileUtil::writeFile(configPath + "schemas/simple_schema.json", ToJsonString(newSchema));
    auto resourceReader =
        ResourceReaderManager::getResourceReader(root + "config/schema_change_config/");
    resourceReader->clearCache();
    doAlterFieldMerge(configPath, root, "11");
    {
        //check price attribute default value
        IndexPartitionOptions options;
        PartitionResourceProviderPtr provider =
            PartitionResourceProviderFactory::GetInstance()->CreateProvider(indexPath, options, "");
        ASSERT_TRUE(provider);
        auto iter = provider->CreatePartitionIterator();
        auto attrIter = iter->CreateSingleAttributeIterator("price", 0);
        ASSERT_TRUE(attrIter);
        ASSERT_EQ(3u, attrIter->GetDocCount());
        for (size_t i = 0; i < 3; i++) {
            ASSERT_TRUE(attrIter->IsValid());
            ASSERT_EQ("0", attrIter->GetValueStr());
            attrIter->MoveToNext();
        }
        ASSERT_FALSE(attrIter->IsValid());
    }
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
}

void NewFieldMergeTaskTest::innerSimpleTest(bool incParallelBuild)
{
    if (incParallelBuild)
    {
        string originIndex = TEST_DATA_PATH"/original_parallel_inc_index/";
        _indexPath = GET_TEST_DATA_PATH() + "/original_parallel_inc_index";
        FileUtil::atomicCopy(originIndex, _indexPath);
    }
    else
    {
        string originIndex = TEST_DATA_PATH"/original_index/";
        _indexPath = GET_TEST_DATA_PATH() + "/original_index_no_parallel";
        FileUtil::atomicCopy(originIndex, _indexPath);
    }
    _configPath = TEST_DATA_PATH"/new_field_merge_task_config/";
    _partitionPath = _indexPath + "/simple/generation_5/partition_0_65535/";
    _kvMap[CONFIG_PATH] = _configPath;
    _kvMap[CLUSTER_NAMES] = "simple";
    _kvMap[GENERATION_ID] = "5";
    _kvMap[INDEX_ROOT_PATH] = _indexPath;
    _kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    _kvMap[MERGE_CONFIG_NAME] = "alter_field";
    _kvMap[MERGE_TIMESTAMP] = "1";
    _kvMap[MERGE_PARALLEL_NUM] = "2";
    _kvMap[ALIGNED_VERSION_ID] = "10";
    _kvMap[WORKER_PATH_VERSION] = "0";
    clearUselessFile();

    {
        NewFieldMergeTaskPtr task1(new NewFieldMergeTask(NULL));
        NewFieldMergeTaskPtr task2(new NewFieldMergeTask(NULL));
        string jobParam = legacy::ToJsonString(_kvMap);
        ASSERT_TRUE(task1->init(jobParam));
        ASSERT_TRUE(task1->run(0, TaskBase::MERGE_MAP));
        task2->init(jobParam);
        ASSERT_TRUE(task1->run(0, TaskBase::MERGE_REDUCE));
        ASSERT_TRUE(task2->run(1, TaskBase::MERGE_REDUCE));
        ASSERT_TRUE(task1->run(0, TaskBase::END_MERGE_MAP));
        IndexPartitionOptions options;
        PartitionResourceProviderPtr provider =
            PartitionResourceProviderFactory::GetInstance()->CreateProvider(_partitionPath, options, "");
        ASSERT_TRUE(provider);
        auto iter = provider->CreatePartitionIterator();
        auto attrIter = iter->CreateSingleAttributeIterator("patch_field2", 0);
        ASSERT_TRUE(attrIter->IsValid());
        ASSERT_EQ("0", attrIter->GetValueStr());
        attrIter->MoveToNext();
        ASSERT_TRUE(attrIter->IsValid());
        ASSERT_EQ("0", attrIter->GetValueStr());
        attrIter->MoveToNext();
        ASSERT_TRUE(attrIter->IsValid());
        ASSERT_EQ("0", attrIter->GetValueStr());
        attrIter->MoveToNext();
        ASSERT_FALSE(attrIter->IsValid());
    }
    _schema = SchemaAdapter::LoadSchema(_configPath + "schemas", "simple_schema.json");
    ASSERT_TRUE(_schema);
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetBuildConfig(false).keepVersionCount = 10;
    ASSERT_TRUE(psm.Init(_schema, options, _partitionPath));
    ASSERT_TRUE(psm.Transfer(BUILD_INC,
                             "cmd=add,pk_field=9,field=9,patch_field1=99,"
                             "patch_field2=999,patch_field3=hello,"
                             "patch_field4=world,patch_field5=30.0 30.1",
                             "patch_index_2:(998,1000)", "docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY,
                             "",
                             "patch_index_2:(998,999)", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "patch_index_5:rectangle(0 0,45 45)", "docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "patch_index_4:world", "docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "pack:world", "docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "expack:hello", "docid=3"));
}


}
}
