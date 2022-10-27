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
#include "indexlib/test/modify_schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
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

class ModifySchemaByOperationTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    //targetDescription=targetVersion;operationId;checkpointVersion
    void doAlterFieldMerge(
            const std::string& configPath, const std::string& root,
            const std::string& targetDescription, string step = "ALL");
    bool checkIndex(
            const IndexPartitionSchemaPtr& schema,
            const std::string& indexRoot, const std::string& query,
            const std::string& result);
    void createVersion(const std::string& incDoc,
                       const IndexPartitionSchemaPtr& schema,
                       const std::string& indexRoot,
                       vector<uint32_t> ongoingOperationIds);
private:
    std::string _indexPath;
    KeyValueMap _kvMap;
    IndexPartitionSchemaPtr _schema;
    IndexPartitionSchemaPtr _newSchema;
};

void ModifySchemaByOperationTest::setUp() {
    _indexPath = GET_TEST_DATA_PATH() + "/simple/generation_0/partition_0_65535";
    string field = "name:string:false:false:uniq;price:uint32;"
        "category:int32:true:true;tags:string:true;coordinate:location";
    string index = "pk:primarykey64:name;index:number:price;spatial_index:spatial:coordinate";
    string attributes = "name;price;category;tags";
    _schema = IndexlibPartitionCreator::CreateSchema(
        "index_table", field, index, attributes, "");
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    auto& buildConfig = options.GetBuildConfig(false);
    buildConfig.keepVersionCount = 10;
    ASSERT_TRUE(psm.Init(_schema, options, _indexPath));
    string docs = "cmd=add,name=doc1,coordinate=90 30,price=10,category=1 2,tags=test1 test,ts=0;"
        "cmd=add,name=doc2,coordinate=90 30,price=20,category=2 3,tags=test2 test,ts=0;"
        "cmd=add,name=doc3,coordinate=90 30,price=30,category=3 4,tags=test3 test,ts=0;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docs, "", ""));
    string incDocs = "cmd=add,name=doc21,coordinate=90 30,price=20,category=2 3,tags=test2 test,ts=0;"
        "cmd=add,name=doc31,coordinate=90 30,price=30,category=3 4,tags=test3 test,ts=0;";
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDocs, "", ""));
    _newSchema.reset(_schema->Clone());
    ModifySchemaMaker::AddModifyOperations(_newSchema,
            "", "nid:uint64;new_nid:uint64", "new_nid:number:new_nid", "nid;new_nid");
    ModifySchemaMaker::AddModifyOperations(_newSchema,
            "attributes=name,price;indexs=index,spatial_index", "nid2:uint64", "", "nid2");
    ModifySchemaMaker::AddModifyOperations(_newSchema,
            "attributes=nid", "nid3:uint64", "index:number:price;"
            "spatial_index:spatial:coordinate", "name;nid3;price");
}

void ModifySchemaByOperationTest::tearDown() {
}

TEST_F(ModifySchemaByOperationTest, testUpdateFields) {
    //prepare segment no doc, only patch 
    string updateDocs = "cmd=update_field,name=doc1,price=11,ts=0;"
                        "cmd=update_field,name=doc2,price=21,ts=0;"
                        "cmd=update_field,name=doc21,price=211,ts=0;";
    createVersion(updateDocs, _schema, _indexPath, {});
    ASSERT_TRUE(checkIndex(_schema, _indexPath, "pk:doc1", "price=11"));
    ASSERT_TRUE(checkIndex(_schema, _indexPath,
                           "spatial_index:rectangle(45 0, 90 40)",
                           "docid=0,coordinate=90 30;docid=1;docid=2;docid=3;docid=4"));

    string configPath = TEST_DATA_PATH"/schema_change_config/";
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
    FileUtil::writeFile(configPath + "schemas/simple_schema.json", ToJsonString(_newSchema));
    //finish alter field
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "3;1;-1");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "3;2;-1");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "3;3;-1");

    string incDoc = "cmd=add,name=doc22,coordinate=45 30,new_nid=1,nid=1,nid2=2,nid3=3,price=23,category=2 3,tags=test2 test,ts=0;";
    createVersion(incDoc, _newSchema, _indexPath, {2,3});
    //delete field in use at once
    ASSERT_TRUE(checkIndex(_schema, _indexPath,
                           "spatial_index:rectangle(45 0, 90 40)", ""));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc1", "price=10"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc1", "price=0"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "price=20"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "price=0"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "index:10", "docid=0"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "index:0", "docid=0;docid=1;docid=2;docid=3"));
    incDoc = "cmd=add,name=doc23,coordinate=45 30,new_nid=1,nid=1,nid2=2,nid3=3,price=23,category=2 3,tags=test2 test,ts=0;";
    createVersion(incDoc, _newSchema, _indexPath, {});
    ASSERT_TRUE(checkIndex(_schema, _indexPath,
                           "spatial_index:rectangle(45 0, 90 40)",
                           "docid=5,coordinate=45 30;docid=6,coordinate=45 30"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "price=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc2", "price=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc21", "price=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "price=23"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc23", "price=23"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "index:23", "docid=5;docid=6"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "index:10", ""));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "index:20", ""));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "index:30", ""));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "index:0", ""));
}

TEST_F(ModifySchemaByOperationTest, testSimple) {
    string configPath = TEST_DATA_PATH"/schema_change_config/";
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
    FileUtil::writeFile(configPath + "schemas/simple_schema.json", ToJsonString(_newSchema));
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "1;1;-1");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;1;1");

    string incDoc = "cmd=add,name=doc22,new_nid=1,nid=1,nid2=2,nid3=3,price=20,category=2 3,tags=test2 test,ts=0;";
    createVersion(incDoc, _newSchema, _indexPath, {2,3});
    //TODO: test nid name
    //check ready fields
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "new_nid=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "new_nid=1"));
    // check delete fields
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc1", "name=doc1"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "name=doc22"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc1", "nid=0"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid=1"));
    // check not ready fields
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid2=2"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid3=3"));

    //alter field finish
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;3;-1");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;2;-1");
    incDoc = "cmd=add,name=doc23,new_nid=1,nid=1,nid2=2,nid3=3,price=20,category=2 3,tags=test2 test,ts=0;";
    createVersion(incDoc, _newSchema, _indexPath, {});
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "new_nid=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "new_nid=1"));
    // check delete fields
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "name="));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "name=doc22"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc1", "nid=0"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid=1"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid2=2"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid3=3"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "nid2=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "nid3=0"));
}

TEST_F(ModifySchemaByOperationTest, testAlterFieldParallel) {
    string configPath = TEST_DATA_PATH"/schema_change_config/";
    FileUtil::removeIfExist(configPath + "schemas/simple_schema.json");
    FileUtil::writeFile(configPath + "schemas/simple_schema.json", ToJsonString(_newSchema));
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;1;-1", "BEGIN_MERGE");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;2;-1", "BEGIN_MERGE");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;1;-1", "DO_MERGE");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;2;-1", "DO_MERGE");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;1;-1", "END_MERGE");
    doAlterFieldMerge(configPath, GET_TEST_DATA_PATH(), "2;2;-1", "END_MERGE");
    
    string incDoc = "cmd=add,name=doc22,new_nid=1,nid=1,nid2=2,nid3=3,price=20,category=2 3,tags=test2 test,ts=0;";
    createVersion(incDoc, _newSchema, _indexPath, {2,3});
    //TODO: test nid name
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc1", "new_nid=0"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "new_nid=1"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid2=2"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid3=3"));
    incDoc = "cmd=add,name=doc23,new_nid=3,nid=3,nid2=3,nid3=3,price=20,category=2 3,tags=test2 test,ts=0;";
    createVersion(incDoc, _newSchema, _indexPath, {3});
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid2=2"));
    ASSERT_TRUE(checkIndex(_newSchema, _indexPath, "pk:doc23", "nid2=3"));
    ASSERT_FALSE(checkIndex(_newSchema, _indexPath, "pk:doc22", "nid3=3"));
}

bool ModifySchemaByOperationTest::checkIndex(
        const IndexPartitionSchemaPtr& schema,
        const std::string& indexRoot, const std::string& query,
        const std::string& result) {
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    psm.Init(schema, options, indexRoot);
    return psm.Transfer(QUERY, "", query, result);
}

void ModifySchemaByOperationTest::createVersion(const std::string& incDoc,
        const IndexPartitionSchemaPtr& schema,
        const std::string& indexRoot,
        vector<uint32_t> ongoingOperationIds) {
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    auto& buildConfig = options.GetBuildConfig(false);
    buildConfig.keepVersionCount = 10;
    options.SetOngoingModifyOperationIds(ongoingOperationIds);
    ASSERT_TRUE(psm.Init(schema, options, indexRoot));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, incDoc, "", ""));
}

void ModifySchemaByOperationTest::doAlterFieldMerge(
        const std::string& configPath, const std::string& root,
        const std::string& targetDescription, std::string step)
{
    string localConfigPath = PathDefine::getLocalConfigPath();
    FileUtil::removeIfExist(localConfigPath);
    vector<string> descriptions;
    StringUtil::fromString(targetDescription, descriptions, ";");

    KeyValueMap kvMap;
    kvMap["targetVersion"] = descriptions[0];
    kvMap["operationId"] = descriptions[1];
    kvMap["checkpointVersion"] = descriptions[2];

    kvMap[CONFIG_PATH] = configPath;
    kvMap[CLUSTER_NAMES] = "simple";
    kvMap[GENERATION_ID] = "0";
    kvMap[INDEX_ROOT_PATH] = root;
    kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    kvMap[MERGE_CONFIG_NAME] = "alter_field";
    kvMap[MERGE_TIMESTAMP] = "1";
    kvMap[MERGE_PARALLEL_NUM] = "1";
    kvMap[TARGET_DESCRIPTION] = targetDescription;
    kvMap[WORKER_PATH_VERSION] = "0";
    NewFieldMergeTaskPtr task(new NewFieldMergeTask(NULL));
    string jobParam = legacy::ToJsonString(kvMap);

    ASSERT_TRUE(task->init(jobParam));
    if (step == "ALL") {
        ASSERT_TRUE(task->run(0, TaskBase::MERGE_MAP));
        ASSERT_TRUE(task->run(0, TaskBase::MERGE_REDUCE));
        ASSERT_TRUE(task->run(0, TaskBase::END_MERGE_MAP));    
    }
    if (step == "BEGIN_MERGE") {
        ASSERT_TRUE(task->run(0, TaskBase::MERGE_MAP));
    }

    if (step == "DO_MERGE") {
        ASSERT_TRUE(task->run(0, TaskBase::MERGE_REDUCE));
    }

    if (step == "END_MERGE") {
        ASSERT_TRUE(task->run(0, TaskBase::END_MERGE_MAP));
    }
}

}
}
