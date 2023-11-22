#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/normal_doc_id_manager.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {

class PatchModifierTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    PatchModifierTest() {}
    ~PatchModifierTest() {}

    DECLARE_CLASS_NAME(PatchModifierTest);

public:
    void CaseSetUp() override
    {
        util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &_options);
    }
    void CaseTearDown() override {}

private:
    void InnerTestGeneratePatch(bool isOldFormatVersion);
    void InnerTestGetTotalMemoryUseForSingleAttrUpdate(bool patchCompress);
    void InnerTestGetTotalMemoryUseForVarNumAttrUpdate(bool patchCompress);
    bool AddDocument(NormalDocIdManager* docIdManager, SegmentWriterPtr writer, NormalDocumentPtr doc);
    bool UpdateDocument(NormalDocIdManager* docIdManager, PatchModifierPtr modifier, NormalDocumentPtr doc);
    bool RemoveDocument(NormalDocIdManager* docIdManager, PatchModifierPtr modifier, NormalDocumentPtr doc);

private:
    IE_LOG_DECLARE();

private:
    config::IndexPartitionOptions _options;
};

INSTANTIATE_TEST_CASE_P(BuildMode, PatchModifierTest, testing::Values(0, 1, 2));

IE_LOG_SETUP(partition, PatchModifierTest);

bool PatchModifierTest::AddDocument(NormalDocIdManager* docIdManager, SegmentWriterPtr writer, NormalDocumentPtr doc)
{
    if (!docIdManager->Process(doc)) {
        return false;
    }
    return writer->AddDocument(doc);
}

bool PatchModifierTest::UpdateDocument(NormalDocIdManager* docIdManager, PatchModifierPtr modifier,
                                       NormalDocumentPtr doc)
{
    if (!docIdManager->Process(doc)) {
        return false;
    }
    return modifier->UpdateDocument(doc);
}

bool PatchModifierTest::RemoveDocument(NormalDocIdManager* docIdManager, PatchModifierPtr modifier,
                                       NormalDocumentPtr doc)
{
    if (!docIdManager->Process(doc)) {
        return false;
    }
    return modifier->RemoveDocument(doc);
}

TEST_P(PatchModifierTest, TestSimpleProcessWithOnDiskPartitionData)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3,4#5,6,7", SFP_OFFLINE);

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    ASSERT_FALSE(modifier.RemoveDocument(8));
    ASSERT_TRUE(modifier.RemoveDocument(0));

    int32_t updateValue = 9;
    StringView value((const char*)&updateValue, sizeof(int32_t));
    ASSERT_FALSE(modifier.UpdateField(9, 1, value, false));
    ASSERT_TRUE(modifier.UpdateField(1, 1, value, false));

    DirectoryPtr segmentDirectory = GET_SEGMENT_DIRECTORY();
    modifier.Dump(segmentDirectory, 2);
    ASSERT_TRUE(segmentDirectory->IsExist("deletionmap/data_0"));
    ASSERT_TRUE(segmentDirectory->IsExist("attribute/field/2_0.patch"));
}

TEST_P(PatchModifierTest, TestRemoveDocument)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_PK_INDEX);
    provider.Build("0,1,2,3,4#5,6,7", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(), PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);
    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments("delete:0,add:8,add:9,delete:9");

    ASSERT_TRUE(RemoveDocument(&manager, modifier, incDocs[0]));
    ASSERT_TRUE(AddDocument(&manager, segWriter, incDocs[1]));
    ASSERT_TRUE(AddDocument(&manager, segWriter, incDocs[2]));
    ASSERT_TRUE(RemoveDocument(&manager, modifier, incDocs[3]));

    DirectoryPtr directory = inMemSegment->GetDirectory();
    modifier->Dump(directory, inMemSegment->GetSegmentId());
    ASSERT_TRUE(directory->IsExist("deletionmap"));
    ASSERT_TRUE(directory->IsExist("deletionmap/data_0"));

    inMemSegment->BeginDump();
    inMemSegment->EndDump();
    ASSERT_TRUE(directory->IsExist("deletionmap/data_2"));
}

TEST_P(PatchModifierTest, TestUpdateDocument)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(), PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);
    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments("update_field:0:3,add:4,update_field:4:6");

    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[0]));
    ASSERT_TRUE(AddDocument(&manager, segWriter, incDocs[1]));
    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[2]));

    DirectoryPtr directory = inMemSegment->GetDirectory();
    modifier->Dump(directory, inMemSegment->GetSegmentId());
    string expectPatchFile = string("attribute/") + attrConfig->GetAttrName() + "/1_0.patch";
    ASSERT_TRUE(directory->IsExist("attribute"));
    ASSERT_TRUE(directory->IsExist(expectPatchFile));

    InMemorySegmentReaderPtr inMemReader = inMemSegment->GetSegmentReader();
    AttributeSegmentReaderPtr inMemAttrReader = inMemReader->GetAttributeSegmentReader(attrConfig->GetAttrName());
    ASSERT_TRUE(inMemAttrReader);

    int32_t value;
    bool isNull = false;
    inMemAttrReader->Read(0, nullptr, (uint8_t*)&value, sizeof(value), isNull);
    ASSERT_EQ((int32_t)6, value);
}

TEST_P(PatchModifierTest, TestUpdateDocumentFailed)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, /*segmentWriter=*/nullptr, PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);

    vector<NormalDocumentPtr> docs = provider.CreateDocuments("update_field:5:3,update_field:3:6,update_field:0:7");

    // check pk not exist update
    ASSERT_FALSE(UpdateDocument(&manager, modifier, docs[0]));

    // check has no pk
    docs[1]->SetIndexDocument(IndexDocumentPtr());
    ASSERT_FALSE(UpdateDocument(&manager, modifier, docs[1]));

    // check no attibute doc
    docs[2]->SetAttributeDocument(AttributeDocumentPtr());
    ASSERT_FALSE(UpdateDocument(&manager, modifier, docs[2]));
}

TEST_P(PatchModifierTest, TestRemoveDocumentFail)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, /*segmentWriter=*/nullptr, PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);

    // check no pk remove
    NormalDocumentPtr doc(new NormalDocument);
    ASSERT_FALSE(RemoveDocument(&manager, modifier, doc));

    // check docid invalid
    ASSERT_FALSE(modifier->RemoveDocument(INVALID_DOCID));
}

TEST_P(PatchModifierTest, TestIsDirty)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    {
        // check delete make modifier dirty
        PatchModifier modifier(schema);
        modifier.Init(partitionData, /*buildResourceMetrics=*/nullptr);
        ASSERT_FALSE(modifier.IsDirty());
        modifier.RemoveDocument(0);
        ASSERT_TRUE(modifier.IsDirty());
    }

    {
        // check update make modifier dirty

        PatchModifierPtr modifier(new PatchModifier(schema));
        modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
        NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
        manager.Reinit(partitionData, modifier, /*segmentWriter=*/nullptr, PartitionWriter::BUILD_MODE_STREAM,
                       /*delayDedupDocument=*/false);

        ASSERT_FALSE(modifier->IsDirty());
        vector<NormalDocumentPtr> docs = provider.CreateDocuments("update_field:1:3");
        ASSERT_TRUE(UpdateDocument(&manager, modifier, docs[0]));
        ASSERT_TRUE(modifier->IsDirty());
    }
}

TEST_P(PatchModifierTest, TestGeneratePatch)
{
    InnerTestGeneratePatch(true);
    InnerTestGeneratePatch(false);
}

void PatchModifierTest::InnerTestGeneratePatch(bool isOldFormatVersion)
{
    tearDown();
    setUp();

    // TODO: delete below when not support old format version
    if (BuildConfig::DEFAULT_USE_PACKAGE_FILE) {
        // not run case when default enablePackageFile is true
        return;
    }

    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);
    // change to old format version
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    string patchFilePath;
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    if (isOldFormatVersion) {
        rootDirectory->RemoveFile(INDEX_FORMAT_VERSION_FILE_NAME);
        IndexFormatVersion binaryVersion("1.8.0");

        binaryVersion.Store(util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), INDEX_FORMAT_VERSION_FILE_NAME),
                            FenceContext::NoFence());
        patchFilePath = string("segment_1/attribute/") + attrConfig->GetAttrName() + "/0.patch";
    } else {
        patchFilePath = string("segment_1/attribute/") + attrConfig->GetAttrName() + "/1_0.patch";
    }

    provider.Build("update_field:0:4", SFP_OFFLINE);
    ASSERT_TRUE(rootDirectory->IsExist(patchFilePath));
}

TEST_P(PatchModifierTest, TestUpdateDocumentWithoutSchema)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "text", SFP_INDEX);
    provider.Build("0", SFP_OFFLINE);
    provider.Build("1", SFP_REALTIME);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, /*segmentWriter=*/nullptr, PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);

    NormalDocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(UPDATE_FIELD);
    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);
    AttributeDocumentPtr attrDoc(new AttributeDocument);
    doc->SetAttributeDocument(attrDoc);

    indexDoc->SetPrimaryKey("0");
    ASSERT_FALSE(UpdateDocument(&manager, modifier, doc));
    indexDoc->SetPrimaryKey("1");
    ASSERT_FALSE(UpdateDocument(&manager, modifier, doc));
}

void PatchModifierTest::InnerTestGetTotalMemoryUseForSingleAttrUpdate(bool patchCompress)
{
    tearDown();
    setUp();

    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    if (patchCompress) {
        ASSERT_TRUE(attrConfig->SetCompressType("patch_compress").IsOK());
    } else {
        attrConfig->TEST_ClearCompressType();
    }

    provider.Build("0,1,2,3", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, /*segmentWriter=*/nullptr, PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments("update_field:0:3,update_field:1:6");

    const util::BuildResourceMetricsPtr& buildMetrics = modifier->GetBuildResourceMetrics();
    int64_t currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    int64_t dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    if (patchCompress) {
        dumpTempMemUseBefore += AttributeUpdater::DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }
    int64_t dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[0]));

    typedef std::unordered_map<docid_t, int32_t> HashMap;
    EXPECT_TRUE(currentMemUseBefore <= buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE));
    EXPECT_EQ(sizeof(docid_t),
              (size_t)(buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore));
    EXPECT_EQ(sizeof(HashMap::value_type),
              (size_t)(buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore));

    currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    // repeat update, not expand
    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[0]));
    // ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[1]));
    EXPECT_EQ(currentMemUseBefore, buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE));
    EXPECT_EQ(dumpTempMemUseBefore, buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE));
    EXPECT_EQ(dumpFileSizeBefore, buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE));
}

TEST_P(PatchModifierTest, TestGetTotalMemoryUseForSingleAttrUpdate)
{
    InnerTestGetTotalMemoryUseForSingleAttrUpdate(true);
    InnerTestGetTotalMemoryUseForSingleAttrUpdate(false);
}

TEST_P(PatchModifierTest, TestGetTotalMemoryUseForVarNumAttrUpdate)
{
    InnerTestGetTotalMemoryUseForVarNumAttrUpdate(true);
    InnerTestGetTotalMemoryUseForVarNumAttrUpdate(false);
}

void PatchModifierTest::InnerTestGetTotalMemoryUseForVarNumAttrUpdate(bool patchCompress)
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "int32:true:true", SFP_ATTRIBUTE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    if (patchCompress) {
        ASSERT_TRUE(attrConfig->SetCompressType("patch_compress").IsOK());
    } else {
        attrConfig->TEST_ClearCompressType();
    }

    provider.Build("0,1,2,3", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    PatchModifierPtr modifier(new PatchModifier(schema));
    modifier->Init(partitionData, /*buildResourceMetrics=*/nullptr);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, /*segmentWriter=*/nullptr, PartitionWriter::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments("update_field:0:34,update_field:0:5");

    const util::BuildResourceMetricsPtr& buildMetrics = modifier->GetBuildResourceMetrics();
    int64_t currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    int64_t dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    int64_t dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[0]));

    // sizeof(header: count) + sizeof(value) * count
    int64_t patchSize = VarNumAttributeFormatter::GetEncodedCountLength(2) + sizeof(int32_t) * 2;

    int64_t diffDumpTempMemSize = sizeof(docid_t);
    if (patchCompress) {
        diffDumpTempMemSize += AttributeUpdater::DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }

#define ESTIMATE_DUMP_FILE_SIZE(isPatchCompress, oriSize)                                                              \
    (isPatchCompress ? config::CompressTypeOption::PATCH_COMPRESS_RATIO * (oriSize) : (oriSize))

    int64_t diffDumpFileSize = ESTIMATE_DUMP_FILE_SIZE(patchCompress, sizeof(docid_t) + patchSize);

    EXPECT_TRUE(patchSize <= buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE) - currentMemUseBefore);
    EXPECT_EQ(diffDumpTempMemSize, buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore);
    EXPECT_EQ(diffDumpFileSize, buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore);

    currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    // repeat update, shorter than last value
    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[1]));
    int64_t newPatchSize = VarNumAttributeFormatter::GetEncodedCountLength(1) + sizeof(int32_t);

    diffDumpTempMemSize = 0;
    diffDumpFileSize = ESTIMATE_DUMP_FILE_SIZE(patchCompress, sizeof(docid_t) + newPatchSize) - diffDumpFileSize;

    EXPECT_EQ(newPatchSize - patchSize, buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE) - currentMemUseBefore);
    EXPECT_EQ(diffDumpTempMemSize, buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore);
    EXPECT_EQ(diffDumpFileSize, buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore);

    currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    patchSize = newPatchSize;
    // repeat update, longer than last value
    ASSERT_TRUE(UpdateDocument(&manager, modifier, incDocs[0]));
    newPatchSize = VarNumAttributeFormatter::GetEncodedCountLength(2) + sizeof(int32_t) * 2;

    diffDumpTempMemSize = 0;
    diffDumpFileSize = ESTIMATE_DUMP_FILE_SIZE(patchCompress, newPatchSize - patchSize);
    EXPECT_EQ(newPatchSize - patchSize, buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE) - currentMemUseBefore);
    EXPECT_EQ(diffDumpTempMemSize, buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore);
    EXPECT_EQ(diffDumpFileSize, buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore);
#undef ESTIMATE_DUMP_FILE_SIZE
}
}} // namespace indexlib::partition
