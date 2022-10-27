#include "indexlib/partition/test/custom_table_intetest.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/test/table_manager.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/document_parser.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomTableTest);

CustomTableTest::CustomTableTest()
{
}

CustomTableTest::~CustomTableTest()
{
}

void CustomTableTest::CaseSetUp()
{
}

void CustomTableTest::CaseTearDown()
{
}

void CustomTableTest::TestSimpleProcess()
{
    string caseDir = GET_TEST_DATA_PATH();
    string indexPluginPath = TEST_DATA_PATH;
    
    string offlineIndexDir =  FileSystemWrapper::JoinPath(
        caseDir, "offline");
    string onlineIndexDir =  FileSystemWrapper::JoinPath(
        caseDir, "online");

    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH,"demo_table_schema.json");
    IndexPartitionOptions options;
    options.GetMergeConfig().SetEnablePackageFile(false);
    TableManager tableManager(
        schema, options, indexPluginPath,
        offlineIndexDir, onlineIndexDir);

    string fullDocString =
        "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;"
        "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;"
        "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2;";

    // create full index
    ASSERT_TRUE(tableManager.Init());
    ASSERT_TRUE(tableManager.BuildFull(fullDocString));
    ResultPtr result = tableManager.Search("k1");
    ASSERT_TRUE(result);
    ASSERT_TRUE(ResultChecker::Check(result, DocumentParser::ParseResult("cfield1=1v1,cfield2=1v2;")));

    // build to offline index 
    string incDoc1 = "cmd=add,pk=k5,cfield1=5v12,cfield2=5v22,ts=2;"
        "cmd=add,pk=k6,cfield1=6v1,cfield2=6v2,ts=2;"
        "cmd=add,pk=k7,cfield1=7v1,cfield2=7v2,ts=2;";
    ASSERT_TRUE(tableManager.BuildInc(incDoc1, TableManager::OfflineBuildFlag::OFB_NO_DEPLOY));

    ASSERT_TRUE(ResultChecker::Check(
                    tableManager.Search("k5"),
                    DocumentParser::ParseResult("cfield1=5v1,cfield2=5v2;")));
    ASSERT_TRUE(ResultChecker::Check(
                    tableManager.Search("k6"),
                    DocumentParser::ParseResult(""))); 
    ASSERT_TRUE(ResultChecker::Check(
                    tableManager.Search("k5", TableManager::SearchFlag::SF_OFFLINE),
                    DocumentParser::ParseResult("cfield1=5v12,cfield2=5v22;")));
    ASSERT_TRUE(tableManager.DeployAndLoadVersion());

    ASSERT_TRUE(ResultChecker::Check(
                    tableManager.Search("k5", TableManager::SearchFlag::SF_ONLINE),
                    DocumentParser::ParseResult("cfield1=5v12,cfield2=5v22;")));    

    string incDoc2 = "cmd=add,pk=k8,cfield1=8v1,cfield2=8v2,ts=4;"
        "cmd=add,pk=k9,cfield1=9v1,cfield2=9v2,ts=4;"
        "cmd=add,pk=kA,cfield1=Av1,cfield2=Av2,ts=4;";

    // build to online index
    ASSERT_TRUE(tableManager.BuildRt(incDoc1 + incDoc2));
    ASSERT_TRUE(tableManager.CheckResult({"k6","k9"}, {"cfield1=6v1,cfield2=6v2", "cfield1=9v1,cfield2=9v2"}));
    ASSERT_TRUE(tableManager.BuildInc(incDoc2));
    ASSERT_TRUE(tableManager.CheckResult({"k6","k9", "kA"}, {"cfield1=6v1,cfield2=6v2", "cfield1=9v1,cfield2=9v2", "cfield1=Av1,cfield2=Av2"}));

    // // build to both offline & online index
    string incDoc3 = "cmd=add,pk=k9,cfield1=9v1,cfield2=9v2,ts=4;"
        "cmd=add,pk=kA,cfield1=Av12,cfield2=Av22,ts=5;"
        "cmd=add,pk=kB,cfield1=Bv1,cfield2=Bv2,ts=6;";
    ASSERT_TRUE(tableManager.BuildIncAndRt(incDoc3));
    ASSERT_TRUE(tableManager.CheckResult({"k9", "kA", "kB"},
                                          {"cfield1=9v1,cfield2=9v2", "cfield1=Av12,cfield2=Av22", "cfield1=Bv1,cfield2=Bv2"}));    
    
}

IE_NAMESPACE_END(partition);

