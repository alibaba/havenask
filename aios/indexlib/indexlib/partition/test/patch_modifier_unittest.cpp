#include "indexlib/partition/test/patch_modifier_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PatchModifierTest);

PatchModifierTest::PatchModifierTest()
{
}

PatchModifierTest::~PatchModifierTest()
{
}

void PatchModifierTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);

    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    
    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    mOptions.SetIsOnline(false);
}

void PatchModifierTest::CaseTearDown()
{
}

void PatchModifierTest::TestSimpleProcessWithOnDiskPartitionData()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3,4#5,6,7", SFP_OFFLINE);

    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    ASSERT_FALSE(modifier.RemoveDocument(8));
    ASSERT_TRUE(modifier.RemoveDocument(0));

    int32_t updateValue = 9;
    ConstString value((const char*)&updateValue, sizeof(int32_t));
    ASSERT_FALSE(modifier.UpdateField(9, 1, value));
    ASSERT_TRUE(modifier.UpdateField(1, 1, value));
    
    DirectoryPtr segmentDirectory = GET_SEGMENT_DIRECTORY();
    modifier.Dump(segmentDirectory, 2);
    ASSERT_TRUE(segmentDirectory->IsExist("deletionmap/data_0"));
    ASSERT_TRUE(segmentDirectory->IsExist("attribute/field/2_0.patch"));
}

void PatchModifierTest::TestRemoveDocument()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_PK_INDEX);
    provider.Build("0,1,2,3,4#5,6,7", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();

    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments(
            "delete:0,add:8,add:9,delete:9");

    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    modifier.RemoveDocument(incDocs[0]);
    segWriter->AddDocument(incDocs[1]);
    segWriter->AddDocument(incDocs[2]);
    modifier.RemoveDocument(incDocs[3]);

    DirectoryPtr directory = inMemSegment->GetDirectory();
    modifier.Dump(directory, inMemSegment->GetSegmentId());
    ASSERT_TRUE(directory->IsExist("deletionmap"));
    ASSERT_TRUE(directory->IsExist("deletionmap/data_0"));

    inMemSegment->BeginDump();
    inMemSegment->EndDump();
    ASSERT_TRUE(directory->IsExist("deletionmap/data_2"));
}

void PatchModifierTest::TestUpdateDocument()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments(
            "update_field:0:3,add:4,update_field:4:6");

    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    modifier.UpdateDocument(incDocs[0]);
    segWriter->AddDocument(incDocs[1]);
    modifier.UpdateDocument(incDocs[2]);

    DirectoryPtr directory = inMemSegment->GetDirectory();
    modifier.Dump(directory, inMemSegment->GetSegmentId());
    string expectPatchFile = 
        string("attribute/") + attrConfig->GetAttrName() + "/1_0.patch";
    ASSERT_TRUE(directory->IsExist("attribute"));
    ASSERT_TRUE(directory->IsExist(expectPatchFile));

    InMemorySegmentReaderPtr inMemReader = inMemSegment->GetSegmentReader();
    AttributeSegmentReaderPtr inMemAttrReader = 
        inMemReader->GetAttributeSegmentReader(attrConfig->GetAttrName());
    ASSERT_TRUE(inMemAttrReader);

    int32_t value;
    inMemAttrReader->Read(0, (uint8_t*)&value, sizeof(value));
    ASSERT_EQ((int32_t)6, value);
}

void PatchModifierTest::TestUpdateDocumentFailed()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    vector<NormalDocumentPtr> docs = provider.CreateDocuments(
            "update_field:5:3,update_field:3:6,update_field:0:7");

    //check pk not exist update
    ASSERT_FALSE(modifier.UpdateDocument(docs[0]));

    //check has no pk
    NormalDocumentPtr doc = docs[1];
    doc->SetIndexDocument(IndexDocumentPtr());
    ASSERT_FALSE(modifier.UpdateDocument(doc));

    //check no attibute doc
    doc = docs[2];
    doc->SetAttributeDocument(AttributeDocumentPtr());
    ASSERT_FALSE(modifier.UpdateDocument(doc)); 
}

void PatchModifierTest::TestRemoveDocumentFail()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);
    // check no pk remove
    DocumentPtr doc(new NormalDocument);
    ASSERT_FALSE(modifier.RemoveDocument(doc));

    // check docid invalid
    ASSERT_FALSE(modifier.RemoveDocument(INVALID_DOCID));
}

void PatchModifierTest::TestIsDirty()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    {
        //check delete make modifier dirty
        PatchModifier modifier(provider.GetSchema());
        modifier.Init(partitionData);
        ASSERT_FALSE(modifier.IsDirty());
        modifier.RemoveDocument(0);
        ASSERT_TRUE(modifier.IsDirty());
    }

    {
        //check update make modifier dirty
        PatchModifier modifier(provider.GetSchema());
        modifier.Init(partitionData);
        ASSERT_FALSE(modifier.IsDirty());
        vector<NormalDocumentPtr> docs = provider.CreateDocuments(
                "update_field:1:3");
        ASSERT_TRUE(modifier.UpdateDocument(docs[0]));
        ASSERT_TRUE(modifier.IsDirty());
    }
}

void PatchModifierTest::TestGeneratePatch()
{
    InnerTestGeneratePatch(true);
    InnerTestGeneratePatch(false);
}

void PatchModifierTest::InnerTestGeneratePatch(bool isOldFormatVersion)
{
    TearDown();
    SetUp();

    // TODO: delete below when not support old format version
    if (BuildConfig::DEFAULT_USE_PACKAGE_FILE)
    {
        // not run case when default enablePackageFile is true
        return;
    }

    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);
    //change to old format version
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    string patchFilePath;
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    if (isOldFormatVersion)
    {
        rootDirectory->RemoveFile(INDEX_FORMAT_VERSION_FILE_NAME);
        IndexFormatVersion binaryVersion("1.8.0");
        binaryVersion.Store(FileSystemWrapper::JoinPath(
                        mRootDir, INDEX_FORMAT_VERSION_FILE_NAME));
        patchFilePath = string("segment_1/attribute/") +
                        attrConfig->GetAttrName() + "/0.patch";
    }
    else
    {
        patchFilePath = string("segment_1/attribute/") +
                        attrConfig->GetAttrName() + "/1_0.patch";
    }

    provider.Build("update_field:0:4", SFP_OFFLINE);
    ASSERT_TRUE(rootDirectory->IsExist(patchFilePath));
}

void PatchModifierTest::TestUpdateDocumentWithoutSchema()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "text", SFP_INDEX);
    provider.Build("0", SFP_OFFLINE);
    provider.Build("1", SFP_REALTIME);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    NormalDocumentPtr doc(new NormalDocument);    
    doc->SetDocOperateType(UPDATE_FIELD);
    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);
    AttributeDocumentPtr attrDoc(new AttributeDocument);
    doc->SetAttributeDocument(attrDoc);

    indexDoc->SetPrimaryKey("0");
    ASSERT_FALSE(modifier.UpdateDocument(doc));
    indexDoc->SetPrimaryKey("1");
    ASSERT_FALSE(modifier.UpdateDocument(doc));
}

void PatchModifierTest::InnerTestGetTotalMemoryUseForSingleAttrUpdate(bool patchCompress)
{
    TearDown();
    SetUp();
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    if (patchCompress)
    {
        attrConfig->GetFieldConfig()->SetCompressType("patch_compress");
    }
    else
    {
        attrConfig->GetFieldConfig()->ClearCompressType();
    }

    provider.Build("0,1,2,3", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments(
            "update_field:0:3,update_field:1:6");

    const util::BuildResourceMetricsPtr& buildMetrics = modifier.GetBuildResourceMetrics();
    int64_t currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    int64_t dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    if (patchCompress)
    {
        dumpTempMemUseBefore += AttributeUpdater::DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }
    int64_t dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);
    
    modifier.UpdateDocument(incDocs[0]);
    typedef std::tr1::unordered_map<docid_t, int32_t> HashMap;
    EXPECT_TRUE(currentMemUseBefore  <= buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE));
    EXPECT_EQ(sizeof(docid_t),
              (size_t)(buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore));
    EXPECT_EQ(sizeof(HashMap::value_type),
              (size_t)(buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore));

    currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    // repeat update, not expand
    modifier.UpdateDocument(incDocs[0]);
    EXPECT_EQ(currentMemUseBefore, buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE));
    EXPECT_EQ(dumpTempMemUseBefore,buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE));
    EXPECT_EQ(dumpFileSizeBefore,buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE));
}

void PatchModifierTest::TestGetTotalMemoryUseForSingleAttrUpdate()
{
    InnerTestGetTotalMemoryUseForSingleAttrUpdate(true);
    InnerTestGetTotalMemoryUseForSingleAttrUpdate(false);
}

void PatchModifierTest::TestGetTotalMemoryUseForVarNumAttrUpdate()
{
    InnerTestGetTotalMemoryUseForVarNumAttrUpdate(true);
    InnerTestGetTotalMemoryUseForVarNumAttrUpdate(false);
}

void PatchModifierTest::InnerTestGetTotalMemoryUseForVarNumAttrUpdate(bool patchCompress)
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32:true:true", SFP_ATTRIBUTE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    if (patchCompress)
    {
        attrConfig->GetFieldConfig()->SetCompressType("patch_compress");
    }
    else
    {
        attrConfig->GetFieldConfig()->ClearCompressType();
    }

    provider.Build("0,1,2,3", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    PatchModifier modifier(provider.GetSchema());
    modifier.Init(partitionData);

    vector<NormalDocumentPtr> incDocs = provider.CreateDocuments(
            "update_field:0:34,update_field:0:5");

    const util::BuildResourceMetricsPtr& buildMetrics = modifier.GetBuildResourceMetrics();
    int64_t currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    int64_t dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    int64_t dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);
    
    modifier.UpdateDocument(incDocs[0]);

    // sizeof(header: count) + sizeof(value) * count
    int64_t patchSize = VarNumAttributeFormatter::GetEncodedCountLength(2)
                + sizeof(int32_t) * 2;

    int64_t diffDumpTempMemSize = sizeof(docid_t);
    if (patchCompress)
    {
        diffDumpTempMemSize += AttributeUpdater::DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }

#define ESTIMATE_DUMP_FILE_SIZE(isPatchCompress, oriSize) \
    (isPatchCompress ? config::CompressTypeOption::PATCH_COMPRESS_RATIO * (oriSize) : (oriSize))
    
    int64_t diffDumpFileSize = ESTIMATE_DUMP_FILE_SIZE(patchCompress, sizeof(docid_t) + patchSize);
    
    EXPECT_TRUE(patchSize <= buildMetrics->GetValue(
                    util::BMT_CURRENT_MEMORY_USE) - currentMemUseBefore);
    EXPECT_EQ(diffDumpTempMemSize, buildMetrics->GetValue(
                    util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore);    
    EXPECT_EQ(diffDumpFileSize, buildMetrics->GetValue(
                    util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore);

    currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);

    // repeat update, shorter than last value
    modifier.UpdateDocument(incDocs[1]);
    int64_t newPatchSize = VarNumAttributeFormatter::GetEncodedCountLength(1)
                + sizeof(int32_t);

    diffDumpTempMemSize = 0;
    diffDumpFileSize = ESTIMATE_DUMP_FILE_SIZE(patchCompress, sizeof(docid_t)+newPatchSize)
                       - diffDumpFileSize;

    EXPECT_EQ(newPatchSize - patchSize,
              buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE) - currentMemUseBefore);
    EXPECT_EQ(diffDumpTempMemSize,
              buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore);
    EXPECT_EQ(diffDumpFileSize,
              buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore);

    currentMemUseBefore = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    dumpTempMemUseBefore = buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpFileSizeBefore = buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE); 


    patchSize = newPatchSize;
    // repeat update, longer than last value
    modifier.UpdateDocument(incDocs[0]);
    newPatchSize = VarNumAttributeFormatter::GetEncodedCountLength(2)
                   + sizeof(int32_t) * 2;

    diffDumpTempMemSize = 0;
    diffDumpFileSize = ESTIMATE_DUMP_FILE_SIZE(patchCompress, newPatchSize - patchSize);
    EXPECT_EQ(newPatchSize - patchSize,
              buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE) - currentMemUseBefore);
    EXPECT_EQ(diffDumpTempMemSize,
              buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE) - dumpTempMemUseBefore);
    EXPECT_EQ(diffDumpFileSize,
              buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE) - dumpFileSizeBefore);
#undef ESTIMATE_DUMP_FILE_SIZE    
}

IE_NAMESPACE_END(partition);

