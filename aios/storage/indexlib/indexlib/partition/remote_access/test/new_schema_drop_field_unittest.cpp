#include "indexlib/partition/remote_access/test/new_schema_drop_field_unittest.h"

#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/testlib/indexlib_partition_creator.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::testlib;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, NewSchemaDropFieldTest);

NewSchemaDropFieldTest::NewSchemaDropFieldTest() {}

NewSchemaDropFieldTest::~NewSchemaDropFieldTest() {}

void NewSchemaDropFieldTest::CaseSetUp()
{
    string field = "name:string;price:uint32;"
                   "category:int32;tags:string:true";
    string index = "pk:primarykey64:name;price_index:number:price";
    string attributes = "price;category;tags";
    string summary = "name;price;category";
    mSchema = IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
    IndexPartitionOptions options;
    string docs = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40,category=4 5,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11,ts=0;"
                  "cmd=update_field,name=doc2,category=2 2,ts=0;";
    mIndexRoot = GET_TEMP_DATA_PATH() + "/index_table/";
    auto ec = FslibWrapper::MkDir(mIndexRoot, false, true).Code();
    THROW_IF_FS_ERROR(ec, "mkdir dir [%s] failed", mIndexRoot.c_str());
    IndexlibPartitionCreator::CreateIndexPartition(mSchema, mIndexRoot, docs, options, "", true);
}

void NewSchemaDropFieldTest::CaseTearDown() {}

bool NewSchemaDropFieldTest::CheckExist(const file_system::IndexFileList& deployMeta, const std::string& path)
{
    for (auto fileMeta : deployMeta.deployFileMetas) {
        if (fileMeta.filePath == path) {
            return true;
        }
    }

    for (auto fileMeta : deployMeta.finalDeployFileMetas) {
        if (fileMeta.filePath == path) {
            return true;
        }
    }
    return false;
}

void NewSchemaDropFieldTest::InnerCheckIndex(const IndexPartitionSchemaPtr& schema, const string& docStr,
                                             const string& query, const string& result)
{
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, mIndexRoot));
    if (docStr.empty()) {
        ASSERT_TRUE(psm.Transfer(QUERY, "", query, result)) << query << "|" << result;
    } else {
        ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docStr, query, result)) << query << "|" << result;
    }
}

void NewSchemaDropFieldTest::PreparePatchIndex(const string& patchIndexPath, const IndexPartitionSchemaPtr& newSchema,
                                               const vector<segmentid_t>& segmentList,
                                               const vector<string>& patchAttribute, const vector<string>& patchIndex,
                                               versionid_t targetVersion)
{
    static size_t schemaId = 0;
    IndexPartitionOptions options;
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(mIndexRoot, options, "");
    auto patchDirectory = resourceProvider->GetPartition()->GetRootDirectory()->MakeDirectory(
        PartitionPatchIndexAccessor::GetPatchRootDirName(++schemaId));
    auto partitionPatcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDirectory);
    for (auto segmentId : segmentList) {
        for (auto attr : patchAttribute) {
            AttributeDataPatcherPtr attrPatcher = partitionPatcher->CreateSingleAttributePatcher(attr, segmentId);
            attrPatcher->Close();
        }
        for (auto index : patchIndex) {
            IndexDataPatcherPtr indexPatcher = partitionPatcher->CreateSingleIndexPatcher(index, segmentId);
            indexPatcher->Close();
        }
    }
    resourceProvider->StoreVersion(newSchema, targetVersion);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();
}

void NewSchemaDropFieldTest::TestSimpleProcess()
{
    InnerCheckIndex(mSchema, "", "price_index:20", "docid=1,price=20,tags=test2 test");
    string field = "name:string;"
                   "category:int32;tags:string:true";
    string index = "pk:primarykey64:name";
    string attributes = "category;tags";
    string sumamry = "name";
    IndexPartitionSchemaPtr newSchema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
    newSchema->SetSchemaVersionId(1);
    PreparePatchIndex("patch_index_1", newSchema, {1}, {}, {}, 2);
    InnerCheckIndex(newSchema, "", "price_index:20", "");

    mSchema->SetSchemaVersionId(2);
    PreparePatchIndex("patch_index_2", mSchema, {1}, {"price"}, {"price_index"}, 3);
    InnerCheckIndex(mSchema, "", "price_index:20", "");
    InnerCheckIndex(mSchema, "", "pk:doc2", "docid=1,price=0,tags=test2 test");
    string doc = "cmd=add,name=doc2,price=21,category=2 3,tags=test2 test,ts=1";
    InnerCheckIndex(mSchema, doc, "price_index:21", "docid=4,price=21,tags=test2 test");
    newSchema->SetSchemaVersionId(3);
    PreparePatchIndex("patch_index_3", newSchema, {1, 2}, {}, {}, 5);
    InnerCheckIndex(newSchema, "", "price_index:21", "");
    InnerCheckIndex(newSchema, "", "pk:doc2", "docid=4,tags=test2 test");
    mSchema->SetSchemaVersionId(4);
    PreparePatchIndex("patch_index_4", mSchema, {1, 2}, {"price"}, {"price_index"}, 6);
    InnerCheckIndex(mSchema, "", "price_index:21", "");
    InnerCheckIndex(mSchema, "", "pk:doc2", "docid=4,price=0,tags=test2 test");
}
}} // namespace indexlib::partition
