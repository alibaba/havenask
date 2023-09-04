#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/partition/partition_validater.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib { namespace partition {

class PartitionValidaterTest : public INDEXLIB_TESTBASE
{
public:
    PartitionValidaterTest();
    ~PartitionValidaterTest();

    DECLARE_CLASS_NAME(PartitionValidaterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    string PrepareData(const string& dirName, PrimaryKeyIndexType pkIndexType, bool keepDuplication);
    void InnerTestValidate(const string& dirName, PrimaryKeyIndexType pkIndexType, bool keepDuplication);

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionValidaterTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.partition, PartitionValidaterTest);

PartitionValidaterTest::PartitionValidaterTest() {}

PartitionValidaterTest::~PartitionValidaterTest() {}

void PartitionValidaterTest::CaseSetUp() {}

void PartitionValidaterTest::CaseTearDown() {}

void PartitionValidaterTest::TestSimpleProcess()
{
    InnerTestValidate("hashTable_normal", pk_hash_table, false);
    InnerTestValidate("hashTable_dup", pk_hash_table, true);

    InnerTestValidate("sortArray_normal", pk_sort_array, false);
    InnerTestValidate("sortArray_dup", pk_sort_array, true);

    InnerTestValidate("blockArray_normal", pk_block_array, false);
    InnerTestValidate("blockArray_dup", pk_block_array, true);
}

void PartitionValidaterTest::InnerTestValidate(const string& dirName, PrimaryKeyIndexType pkIndexType,
                                               bool keepDuplication)
{
    string indexPath = PrepareData(dirName, pkIndexType, keepDuplication);
    IndexPartitionOptions options;
    PartitionValidater validater(options);
    ASSERT_TRUE(validater.Init(indexPath));

    if (keepDuplication) {
        ASSERT_THROW(validater.Check(), util::IndexCollapsedException);
    } else {
        ASSERT_NO_THROW(validater.Check());
    }
}

string PartitionValidaterTest::PrepareData(const string& dirName, PrimaryKeyIndexType pkIndexType, bool keepDuplication)
{
    string field = "nid:string;price:uint32";
    string index = "pk:primarykey64:nid";
    string attribute = "price";

    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode;
    if (pkIndexType == pk_hash_table) {
        loadMode = PrimaryKeyLoadStrategyParam::HASH_TABLE;
    } else if (pkIndexType == pk_block_array) {
        loadMode = PrimaryKeyLoadStrategyParam::BLOCK_VECTOR;
    } else {
        loadMode = PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
    }

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    PrimaryKeyIndexConfigPtr pkIndex =
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    pkIndex->SetPrimaryKeyIndexType(pkIndexType);
    pkIndex->SetPrimaryKeyLoadParam(loadMode, true);
    schema->SetAutoUpdatePreference(false);

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig(false).maxDocCount = 2;
    options.GetBuildConfig(false).enablePackageFile = false;

    string docString = "cmd=add,nid=1,price=1,locator=0,ts=1;"
                       "cmd=add,nid=2,price=2,locator=1,ts=2;"
                       "cmd=add,nid=3,price=3,locator=2,ts=3;"
                       "cmd=add,nid=2,price=4,locator=3,ts=4;";

    string indexPath = GET_TEMP_DATA_PATH() + "/" + dirName;
    PartitionStateMachine psm;
    psm.Init(schema, options, indexPath);
    psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", "");

    if (keepDuplication) {
        // delay dedup
        string segmentName = (pkIndexType == pk_hash_table) ? "segment_1_level_0" : "segment_2_level_0";
        string fileName = segmentName + "/deletionmap/data_0";
        auto ret = FslibWrapper::DeleteFile(indexPath + "/" + fileName, DeleteOption::NoFence(false)).Code();
        assert(FSEC_OK == ret);
        (void)ret;

        DirectoryPtr rootDir = GET_CHECK_DIRECTORY(false)->GetDirectory(dirName, false);
        DirectoryPtr segDir = rootDir->GetDirectory(segmentName, false);
        DirectoryPtr deletionMapDir = segDir->GetDirectory("deletionmap", false);
        deletionMapDir->RemoveFile("data_0");
        DeletionMapSegmentWriter segWriter;
        segWriter.Init(2);
        segWriter.Dump(deletionMapDir, 0, true);
    }
    return indexPath;
}

}} // namespace indexlib::partition
