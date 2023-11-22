#include "indexlib/index_base/patch/test/partition_patch_meta_creator_unittest.h"

#include "indexlib/config/test/modify_schema_maker.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::testlib;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionPatchMetaCreatorTest);

PartitionPatchMetaCreatorTest::PartitionPatchMetaCreatorTest() {}

PartitionPatchMetaCreatorTest::~PartitionPatchMetaCreatorTest() {}

void PartitionPatchMetaCreatorTest::CaseSetUp()
{
    string field = "name:string:false:false:uniq;price:uint32;category:int32:true:true;tags:string:true";
    string index = "pk:primarykey64:name;tag_index:string:tags;cat_index:number:category";
    string attributes = "name;price;category;tags";
    mSchema = IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
}

void PartitionPatchMetaCreatorTest::CaseTearDown() {}

void PartitionPatchMetaCreatorTest::TestSimpleProcess()
{
    mSchema->SetSchemaVersionId(1);
    Version version = CreateVersion("0,1,2", 0);
    version.SetSchemaVersionId(1);

    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 1,
                      "0:tag_index,cat_index:price;1:tag_index:tags;2:cat_index:category");

    PartitionPatchMetaPtr patchMeta = PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, version);
    CheckPatchMeta(patchMeta, 1, "0:tag_index,cat_index:price;1:tag_index:tags;2:cat_index:category");
}

void PartitionPatchMetaCreatorTest::TestSchemaWithModifyOperations()
{
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload";
    IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");

    IndexPartitionSchemaPtr oldSchema(schema->Clone());
    test::ModifySchemaMaker::AddModifyOperations(schema, "attributes=tags;indexs=categoryIndex",
                                                 "new_int:uint64;new_string:string", "newIntIndex:number:new_int",
                                                 "new_int;new_string");

    // opId = 1, delete: attribute=tags; index=categoryIndex
    //           add:    attribute=new_int,new_string; index=newIntIndex
    IndexPartitionSchemaPtr currentSchema(schema->Clone());

    // opId = 2, delete: attribute=category,new_string
    //           add:    attribute=new_multi_int,tags; index=newMultiIntIndex
    test::ModifySchemaMaker::AddModifyOperations(schema, "attributes=category,new_string;",
                                                 "new_multi_int:uint64:true:true",
                                                 "newMultiIntIndex:number:new_multi_int", "new_multi_int;tags");
    IndexPartitionSchemaPtr newSchema(schema->Clone());
    Version version = CreateVersion("0,1,2", 0);
    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 1,
                      "0:newIntIndex:new_int,new_string;"
                      "1:newIntIndex:new_int,new_string;"
                      "2:newIntIndex:new_int,new_string");

    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 2,
                      "0:newMultiIntIndex:new_multi_int,tags;"
                      "1:newMultiIntIndex:new_multi_int,tags;"
                      "2:newMultiIntIndex:new_multi_int,tags");

    {
        version.SyncSchemaVersionId(currentSchema);
        PartitionPatchMetaPtr patchMeta =
            PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), currentSchema, version);
        CheckPatchMeta(patchMeta, 1,
                       "0:newIntIndex:new_int,new_string;"
                       "1:newIntIndex:new_int,new_string;"
                       "2:newIntIndex:new_int,new_string");
    }

    {
        version.SyncSchemaVersionId(newSchema);
        PartitionPatchMetaPtr patchMeta =
            PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), newSchema, version);
        CheckPatchMeta(patchMeta, 1,
                       "0:newIntIndex:new_int;"
                       "1:newIntIndex:new_int;"
                       "2:newIntIndex:new_int");
        CheckPatchMeta(patchMeta, 2,
                       "0:newMultiIntIndex:new_multi_int,tags;"
                       "1:newMultiIntIndex:new_multi_int,tags;"
                       "2:newMultiIntIndex:new_multi_int,tags");
    }

    {
        version.SyncSchemaVersionId(newSchema);
        version.SetOngoingModifyOperations({1});
        PartitionPatchMetaPtr patchMeta =
            PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), newSchema, version);
        CheckPatchMeta(patchMeta, 1, "");
        CheckPatchMeta(patchMeta, 2,
                       "0:newMultiIntIndex:new_multi_int,tags;"
                       "1:newMultiIntIndex:new_multi_int,tags;"
                       "2:newMultiIntIndex:new_multi_int,tags");
    }
}

void PartitionPatchMetaCreatorTest::TestSchemaAndVersionNotSameSchemaVersion()
{
    Version version = CreateVersion("0,1,2", 0);
    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 1,
                      "0:tag_index,cat_index:price;1:tag_index:tags;2:cat_index:category");
    // get null for DEFAULT_SCHEMAID
    ASSERT_FALSE(PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, version));

    // throw exception when not same schema id
    version.SetSchemaVersionId(2);
    mSchema->SetSchemaVersionId(1);
    ASSERT_ANY_THROW(PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, version));
}

void PartitionPatchMetaCreatorTest::TestMultiSchemaIdPatchMeta()
{
    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 1,
                      "0:tag_index,cat_index:price;1:tag_index:tags,category;2:cat_index:category,name");
    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 2, "1:tag_index,cat_index:tags,price;2:cat_index:category");

    Version version = CreateVersion("0,1,2", 0);
    version.SetSchemaVersionId(2);
    mSchema->SetSchemaVersionId(2);

    PartitionPatchMetaPtr patchMeta = PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, version);
    CheckPatchMeta(patchMeta, 2, "1:tag_index,cat_index:tags,price;2:cat_index:category");
    CheckPatchMeta(patchMeta, 1, "0:tag_index,cat_index:price;1::category;2::name");
}

void PartitionPatchMetaCreatorTest::TestMultiSchemaIdPatchMetaWithLastVersion()
{
    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 1,
                      "0:tag_index,cat_index:price;1:tag_index:tags,category;2:cat_index:category,name");
    Version version = CreateVersion("0,1,2", 0);
    version.SetSchemaVersionId(1);
    mSchema->SetSchemaVersionId(1);
    PartitionPatchMetaPtr patchMeta = PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, version);
    patchMeta->Store(GET_PARTITION_DIRECTORY(), 0);
    version.Store(GET_PARTITION_DIRECTORY(), true);

    Version newVersion = CreateVersion("0,1,2", 1);
    newVersion.SetSchemaVersionId(2);
    mSchema->SetSchemaVersionId(2);
    PreparePatchIndex(GET_PARTITION_DIRECTORY(), 2, "1:tag_index,cat_index:tags,price;2:cat_index:category");
    patchMeta = PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, newVersion);
    CheckPatchMeta(patchMeta, 2, "1:tag_index,cat_index:tags,price;2:cat_index:category");
    CheckPatchMeta(patchMeta, 1, "0:tag_index,cat_index:price;1::category;2::name");
}

void PartitionPatchMetaCreatorTest::TestWithInvalidIndexAndAttribute()
{
    mSchema->SetSchemaVersionId(1);
    Version version = CreateVersion("0,1,2", 0);
    version.SetSchemaVersionId(1);

    PreparePatchIndex(
        GET_PARTITION_DIRECTORY(), 1,
        "0:tag_index,cat_index,invalid_index:price;1:tag_index:tags;2:cat_index:category,invalid_attribute");

    PartitionPatchMetaPtr patchMeta = PartitionPatchMetaCreator::Create(GET_PARTITION_DIRECTORY(), mSchema, version);
    CheckPatchMeta(patchMeta, 1, "0:tag_index,cat_index:price;1:tag_index:tags;2:cat_index:category");
}

Version PartitionPatchMetaCreatorTest::CreateVersion(const string& versionStr, versionid_t versionId)
{
    Version version(versionId);
    vector<segmentid_t> segIds;
    StringUtil::fromString(versionStr, segIds, ",");
    for (auto segId : segIds) {
        version.AddSegment(segId);
    }
    return version;
}

void PartitionPatchMetaCreatorTest::PreparePatchIndex(const DirectoryPtr& rootDir, schemaid_t schemaId,
                                                      const string& dataStr)
{
    DirectoryPtr patchIndexRoot = rootDir->MakeDirectory(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));

    vector<vector<string>> patchIndexInfos;
    StringUtil::fromString(dataStr, patchIndexInfos, ":", ";");

    for (auto patchIndexInfo : patchIndexInfos) {
        assert(patchIndexInfo.size() == 3);
        string segName = string("segment_") + patchIndexInfo[0] + "_level_0";
        DirectoryPtr segPath = patchIndexRoot->MakeDirectory(segName);

        DirectoryPtr indexRootPath = segPath->MakeDirectory(INDEX_DIR_NAME);
        vector<string> indexs;
        StringUtil::fromString(patchIndexInfo[1], indexs, ",");
        for (auto index : indexs) {
            indexRootPath->MakeDirectory(index);
        }

        DirectoryPtr attrRootPath = segPath->MakeDirectory(ATTRIBUTE_DIR_NAME);
        vector<string> attrs;
        StringUtil::fromString(patchIndexInfo[2], attrs, ",");
        for (auto attr : attrs) {
            attrRootPath->MakeDirectory(attr);
        }
    }
}

void PartitionPatchMetaCreatorTest::CheckPatchMeta(const PartitionPatchMetaPtr& patchMeta, schemaid_t schemaId,
                                                   const string& dataStr)
{
    ASSERT_TRUE(patchMeta);
    SchemaPatchInfoPtr schemaInfo = patchMeta->FindSchemaPatchInfo(schemaId);
    if (dataStr.empty()) {
        ASSERT_FALSE(schemaInfo);
        return;
    }

    ASSERT_TRUE(schemaInfo);
    ASSERT_EQ(schemaId, schemaInfo->GetSchemaId());

    vector<string> patchIndexInfos = StringUtil::split(dataStr, ";");
    ASSERT_EQ(patchIndexInfos.size(), schemaInfo->GetPatchSegmentSize());

    size_t idx = 0;
    SchemaPatchInfo::Iterator iter = schemaInfo->Begin();
    for (; iter != schemaInfo->End(); iter++) {
        vector<string> patchIndexInfo = StringUtil::split(patchIndexInfos[idx], ":", false);
        assert(patchIndexInfo.size() == 3);

        segmentid_t segId = StringUtil::fromString<segmentid_t>(patchIndexInfo[0]);
        ASSERT_EQ(segId, iter->GetSegmentId());

        vector<string> indexs = StringUtil::split(patchIndexInfo[1], ",");
        sort(indexs.begin(), indexs.end());
        vector<string> expectIndexs;
        expectIndexs = iter->GetPatchIndexs();
        sort(expectIndexs.begin(), expectIndexs.end());
        ASSERT_EQ(expectIndexs, indexs);

        vector<string> attrs = StringUtil::split(patchIndexInfo[2], ",");
        sort(attrs.begin(), attrs.end());
        vector<string> expectAttrs;
        expectAttrs = iter->GetPatchAttributes();
        sort(expectAttrs.begin(), expectAttrs.end());
        ASSERT_EQ(expectAttrs, attrs);
        ++idx;
    }
}
}} // namespace indexlib::index_base
