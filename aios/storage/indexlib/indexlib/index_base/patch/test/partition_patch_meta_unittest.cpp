#include "indexlib/index_base/patch/test/partition_patch_meta_unittest.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionPatchMetaTest);

PartitionPatchMetaTest::PartitionPatchMetaTest() {}

PartitionPatchMetaTest::~PartitionPatchMetaTest() {}

void PartitionPatchMetaTest::CaseSetUp() {}

void PartitionPatchMetaTest::CaseTearDown() {}

void PartitionPatchMetaTest::TestSimpleProcess()
{
    PartitionPatchMetaPtr patchMeta(new PartitionPatchMeta);
    patchMeta->AddPatchIndex(2, 0, "index3");
    patchMeta->AddPatchIndex(2, 0, "index4");
    patchMeta->AddPatchIndex(2, 1, "index3");
    patchMeta->AddPatchIndex(2, 1, "index4");
    patchMeta->AddPatchIndex(2, 1, "index5");
    patchMeta->AddPatchIndex(1, 0, "index1");

    patchMeta->AddPatchAttribute(2, 0, "attr3");
    patchMeta->AddPatchAttribute(2, 1, "attr4");
    patchMeta->AddPatchAttribute(1, 0, "attr1");

    patchMeta->Store(GET_PARTITION_DIRECTORY(), 4);

    PartitionPatchMetaPtr newPatchMeta(new PartitionPatchMeta);
    newPatchMeta->Load(GET_PARTITION_DIRECTORY(), 4);

    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByIndexName(0, "index3"));
    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByIndexName(0, "index4"));

    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByIndexName(1, "index3"));
    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByIndexName(1, "index4"));
    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByIndexName(1, "index5"));

    ASSERT_EQ(1, newPatchMeta->GetSchemaIdByIndexName(0, "index1"));

    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByAttributeName(0, "attr3"));
    ASSERT_EQ(2, newPatchMeta->GetSchemaIdByAttributeName(1, "attr4"));

    ASSERT_EQ(1, newPatchMeta->GetSchemaIdByAttributeName(0, "attr1"));

    ASSERT_EQ(DEFAULT_SCHEMAID, newPatchMeta->GetSchemaIdByAttributeName(1, "no_exist"));
    ASSERT_EQ(DEFAULT_SCHEMAID, newPatchMeta->GetSchemaIdByAttributeName(5, "attr3"));

    ASSERT_EQ(DEFAULT_SCHEMAID, newPatchMeta->GetSchemaIdByIndexName(0, "no_exist"));
    ASSERT_EQ(DEFAULT_SCHEMAID, newPatchMeta->GetSchemaIdByIndexName(6, "index2"));

    vector<SchemaPatchInfoPtr> schemaMetas;
    PartitionPatchMeta::Iterator iter = newPatchMeta->CreateIterator();
    while (iter.HasNext()) {
        schemaMetas.push_back(iter.Next());
    }
    ASSERT_EQ((size_t)2, schemaMetas.size());
    ASSERT_EQ((schemavid_t)2, schemaMetas[0]->GetSchemaId());
    ASSERT_EQ((schemavid_t)1, schemaMetas[1]->GetSchemaId());

    ASSERT_EQ(schemaMetas[0].get(), newPatchMeta->FindSchemaPatchInfo(2).get());
    ASSERT_EQ(schemaMetas[1].get(), newPatchMeta->FindSchemaPatchInfo(1).get());
    ASSERT_EQ(NULL, newPatchMeta->FindSchemaPatchInfo(3).get());

    vector<schemavid_t> schemaIds;
    newPatchMeta->GetSchemaIdsBySegmentId(0, schemaIds);
    ASSERT_EQ((size_t)2, schemaIds.size());
    ASSERT_EQ((schemavid_t)2, schemaIds[0]);
    ASSERT_EQ((schemavid_t)1, schemaIds[1]);

    schemaIds.clear();
    newPatchMeta->GetSchemaIdsBySegmentId(1, schemaIds);
    ASSERT_EQ((size_t)1, schemaIds.size());
    ASSERT_EQ((schemavid_t)2, schemaIds[0]);
}
}} // namespace indexlib::index_base
