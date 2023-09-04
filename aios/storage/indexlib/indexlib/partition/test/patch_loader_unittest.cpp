#include "indexlib/partition/test/patch_loader_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PatchLoaderTest);

class SimpleAttrPatchWorkItem : public AttributePatchWorkItem
{
public:
    SimpleAttrPatchWorkItem(const std::string& id, size_t patchItemCount, bool isSub, PatchWorkItemType itemType)
        : AttributePatchWorkItem(id, patchItemCount, isSub, itemType)
        , mCount(0)
    {
    }

    ~SimpleAttrPatchWorkItem() {}

    bool Init(const index::DeletionMapReaderPtr& deletionMap,
              const index::InplaceAttributeModifierPtr& modifier) override
    {
        return true;
    }

    bool HasNext() const override { return mCount < _patchCount; }

    size_t ProcessNext() override
    {
        ++mCount;
        return 1;
    }
    void destroy() override {}
    void drop() override {}

public:
    size_t mCount;
};

PatchLoaderTest::PatchLoaderTest() {}

PatchLoaderTest::~PatchLoaderTest() {}

void PatchLoaderTest::CaseSetUp()
{
    string field = "pk:string;cat:uint32:true:true;single_value:uint32";
    string index = "pk:primarykey64:pk";
    string attr = "cat;single_value";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");

    string subField = "subPk:string;subCat:uint32:true:true;sub_single_value:uint32";
    string subIndex = "subPk:primarykey64:subPk";
    string subAttr = "subCat;sub_single_value";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subField, subIndex, subAttr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::RewriteForSubTable(mSchema);
    const IFileSystemPtr& fs = GET_FILE_SYSTEM();
    mRootDir = DirectoryCreator::Create(fs, "test");
}

void PatchLoaderTest::CaseTearDown() {}

void PatchLoaderTest::PrepareSegment(segmentid_t segId, bool isCompressed)
{
    string segName = string("segment_") + StringUtil::toString(segId) + "_level_0";
    DirectoryPtr segDir = mRootDir->MakeDirectory(segName);
    PrepareSegmentData(segId, segDir, "cat", false, isCompressed);
    PrepareSegmentData(segId, segDir, "single_value", false, isCompressed);

    DirectoryPtr subSegDir = segDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    PrepareSegmentData(segId, subSegDir, "subCat", true, isCompressed);
    PrepareSegmentData(segId, subSegDir, "sub_single_value", true, isCompressed);
}

void PatchLoaderTest::MakeFile(const DirectoryPtr& dir, const string& fileName, size_t bytes, bool isCompressed)
{
    string content;
    content.resize(bytes, 'a');
    if (!isCompressed) {
        dir->Store(fileName, content);
        return;
    }
    FileWriterPtr fileWriter = dir->CreateFileWriter(fileName);
    SnappyCompressFileWriter compressWriter;
    compressWriter.Init(fileWriter);
    compressWriter.Write(content.c_str(), bytes).GetOrThrow();
    ASSERT_EQ(FSEC_OK, compressWriter.Close());
}

void PatchLoaderTest::PrepareSegmentData(segmentid_t segId, DirectoryPtr segDir, const string& attrName, bool isSub,
                                         bool isCompressed)
{
    DirectoryPtr attrDir = segDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    DirectoryPtr catAttrDir = attrDir->MakeDirectory(attrName);
    segmentid_t src = segId;
    for (segmentid_t dst = 0; dst < segId; dst++) {
        string patchFileName = StringUtil::toString(src) + "_" + StringUtil::toString(dst) + "." + PATCH_FILE_NAME;
        size_t length = (src + dst) * 16;
        if (isSub) {
            length *= 2;
        }
        MakeFile(catAttrDir, patchFileName, length, isCompressed);
    }

    if (!segDir->IsExist("segment_info")) {
        SegmentInfo segInfo;
        segInfo.Store(segDir);
    }
}

void PatchLoaderTest::TestCalculatePatchFileSize()
{
    // segment_0
    // segment_1 : 1_0.patch
    // segment_2 : 2_0.patch 2_1.patch
    // segment_3 : 3_0.patch 3_1.patch 3_2.patch
    // segment_4 : 4_0.patch 4_1.patch 4_2.patch 4_3.patch
    // segment_5 : 5_0.patch 5_1.patch 5_2.patch 5_3.patch 5_4.patch
    Version version;
    for (segmentid_t segId = 0; segId <= 5; segId++) {
        PrepareSegment(segId);
        version.AddSegment(segId);
    }

    Version lastLoadVersion;
    for (segmentid_t segId = 0; segId <= 3; segId++) {
        lastLoadVersion.AddSegment(segId);
    }

    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        mRootDir->GetFileSystem(), version, mRootDir->GetLogicalPath(), true, false);
    {
        IndexPartitionOptions options;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
        // normal case, main + sub(sub = main * 2)
        size_t expectSize = (4 + 5 + 6 + 7 + 5 + 6 + 7 + 8 + 9) * 16 * 3;
        ASSERT_EQ(expectSize, patchLoader.CalculatePatchLoadExpandSize());
    }
    {
        // test src filter by start segmentId
        DirectoryPtr seg4CatDir = mRootDir->GetDirectory("segment_4_level_0/attribute/cat", true);
        mRootDir->RemoveFile("segment_2_level_0/attribute/cat/2_0.patch");
        mRootDir->RemoveFile("segment_3_level_0/attribute/cat/3_2.patch");
        MakeFile(seg4CatDir, "2_0.patch", 2);
        MakeFile(seg4CatDir, "3_2.patch", 5);
        IndexPartitionOptions options;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
        // normal case, main + sub(sub = main * 2)
        size_t expectSize = (4 + 5 + 6 + 7 + 5 + 6 + 7 + 8 + 9) * 16 * 3;
        ASSERT_EQ(expectSize, patchLoader.CalculatePatchLoadExpandSize());
    }

    // test dst not exist
    {
        version.RemoveSegment(2);
        partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(mRootDir->GetFileSystem(), version,
                                                                       mRootDir->GetLogicalPath(), true, false);

        size_t expectSize = (4 + 5 + 7 + 5 + 6 + 8 + 9) * 16 * 3; // sub 4_2.patch, 5_2.patch
        IndexPartitionOptions options;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
        ASSERT_EQ(expectSize, patchLoader.CalculatePatchLoadExpandSize());
    }
    {
        // test main attribute not updatable
        string field = "pk:string;cat:uint32:true";
        string index = "pk:primarykey64:pk";
        string attr = "cat";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, "");
        string subField = "subPk:string;subCat:uint32:true";
        string subIndex = "subPk:primarykey64:subPk";
        string subAttr = "subCat";
        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subField, subIndex, subAttr, "");
        schema->SetSubIndexPartitionSchema(subSchema);
        SchemaRewriter::RewriteForSubTable(schema);
        IndexPartitionOptions options;
        PatchLoader patchLoader(schema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
        ASSERT_EQ(0u, patchLoader.CalculatePatchLoadExpandSize());
    }
}

void PatchLoaderTest::TestCalculatePatchFileSizeWithCompress()
{
    string field = "pk:string;cat:uint32:true:true:patch_compress";
    string index = "pk:primarykey64:pk";
    string attr = "cat";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");

    string subField = "subPk:string;subCat:uint32:true:true:patch_compress";
    string subIndex = "subPk:primarykey64:subPk";
    string subAttr = "subCat";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subField, subIndex, subAttr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::RewriteForSubTable(mSchema);
    // segment_0
    // segment_1 : 1_0.patch
    // segment_2 : 2_0.patch 2_1.patch
    // segment_3 : 3_0.patch 3_1.patch 3_2.patch
    // segment_4 : 4_0.patch 4_1.patch 4_2.patch 4_3.patch
    // segment_5 : 5_0.patch 5_1.patch 5_2.patch 5_3.patch 5_4.patch
    Version version;
    for (segmentid_t segId = 0; segId <= 5; segId++) {
        PrepareSegment(segId, true);
        version.AddSegment(segId);
    }

    Version lastLoadVersion;
    for (segmentid_t segId = 0; segId <= 3; segId++) {
        lastLoadVersion.AddSegment(segId);
    }

    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        mRootDir->GetFileSystem(), version, mRootDir->GetLogicalPath(), true, false);
    IndexPartitionOptions options;
    PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
    patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
    // normal case, main + sub(sub = main * 2)
    size_t expectSize = (4 + 5 + 6 + 7 + 5 + 6 + 7 + 8 + 9) * 16 * 3;
    ASSERT_EQ(expectSize, patchLoader.CalculatePatchLoadExpandSize());
}

void PatchLoaderTest::TestCalculatePackAttributeUpdateExpandSize()
{
    string field = "pk:string;id:uint32;price:uint32";
    string index = "pk:primarykey64:pk";
    string attribute = "packAttr:id,price";
    // size of one pack is 4 + 4 + 1(count) = 9
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string sub_field = "sub_pk:string;sub_id:uint64;sub_price:uint32";
    string sub_index = "sub_pk:primarykey64:sub_pk";
    string sub_attribute = "sub_packAttr:sub_id,sub_price:uniq";
    // size of one pack is 8 + 4 + 1(count) = 13

    mSchema->SetSubIndexPartitionSchema(SchemaMaker::MakeSchema(sub_field, sub_index, sub_attribute, ""));
    SchemaRewriter::RewriteForSubTable(mSchema);

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));

    // full : pk1(spk11,spk12) + pk2(spk21,spk22)
    string fullDoc = "cmd=add,pk=pk1,id=1,price=1,sub_pk=spk11^spk12,sub_id=1^1,sub_price=11^12,ts=1;"
                     "cmd=add,pk=pk2,id=2,price=2,sub_pk=spk21^spk22,sub_id=1^2,sub_price=11^22,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    // inc1 : pk3(spk31) + update pk1(id)/ spk11(sub_id)
    string inc1Doc = "cmd=add,pk=pk3,id=3,price=3,sub_pk=spk31,sub_id=3,sub_price=31,ts=2;"
                     "cmd=update_field,pk=pk1,id=11,sub_pk=spk11,sub_id=3,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, inc1Doc, "", ""));
    OnDiskPartitionDataPtr partData = OnDiskPartitionData::CreateOnDiskPartitionData(psm.GetFileSystem(), mSchema);
    size_t expectUpdateDocSize =
        9 + 13 + CalculatePatchFileSize(partData, mSchema->GetAttributeSchema()->GetAttributeConfig("id")) +
        CalculatePatchFileSize(
            partData->GetSubPartitionData(),
            mSchema->GetSubIndexPartitionSchema()->GetAttributeSchema()->GetAttributeConfig("sub_id")) +
        CalculatePatchFileSize(
            partData->GetSubPartitionData(),
            mSchema->GetSubIndexPartitionSchema()->GetAttributeSchema()->GetAttributeConfig("sub_price"));
    PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
    patchLoader.Init(partData, false, index_base::Version(INVALID_VERSION), 0, false);
    ASSERT_EQ(expectUpdateDocSize, patchLoader.CalculatePatchLoadExpandSize());
}

size_t PatchLoaderTest::CalculatePatchFileSize(const PartitionDataPtr& partitionData,
                                               const AttributeConfigPtr& attrConfig)
{
    PatchFileFinderPtr finder = PatchFileFinderCreator::Create(partitionData.get());
    PatchInfos patchInfos;
    finder->FindAttrPatchFiles(attrConfig, &patchInfos);
    PatchInfos::iterator iter = patchInfos.begin();
    size_t patchFileSize = 0;
    for (; iter != patchInfos.end(); iter++) {
        PatchFileInfoVec& patchFiles = iter->second;
        for (size_t i = 0; i < patchFiles.size(); i++) {
            patchFileSize += patchFiles[i].patchDirectory->GetFileLength(patchFiles[i].patchFileName);
        }
    }
    return patchFileSize;
}

void PatchLoaderTest::TestInitWithLoadPatchThreadNum()
{
    // segment_0
    // segment_1 : 1_0.patch
    // segment_2 : 2_0.patch 2_1.patch
    // segment_3 : 3_0.patch 3_1.patch 3_2.patch
    // segment_4 : 4_0.patch 4_1.patch 4_2.patch 4_3.patch
    // segment_5 : 5_0.patch 5_1.patch 5_2.patch 5_3.patch 5_4.patch

    Version version;
    for (segmentid_t segId = 0; segId <= 5; segId++) {
        PrepareSegment(segId);
        version.AddSegment(segId);
    }

    Version lastLoadVersion;
    for (segmentid_t segId = 0; segId <= 3; segId++) {
        lastLoadVersion.AddSegment(segId);
    }

    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        mRootDir->GetFileSystem(), version, mRootDir->GetLogicalPath(), true, false);

    {
        IndexPartitionOptions options;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
        ASSERT_EQ(0u, patchLoader._patchWorkItems.size());
    }
    {
        IndexPartitionOptions options;
        options.GetOnlineConfig().loadPatchThreadNum = 1;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, false);
        // normal case, main + sub(sub = main * 2)
        ASSERT_EQ(0u, patchLoader._patchWorkItems.size());
    }
    {
        IndexPartitionOptions options;
        options.GetOnlineConfig().loadPatchThreadNum = 3;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, true, lastLoadVersion, 4, true);
        // test is_inc_consistent_with_rt = true, startLoad = 4
        // itemCount = 4 * 1 (4fields * 1dstSegIds)
        ASSERT_EQ(4u, patchLoader._patchWorkItems.size());
    }
    {
        IndexPartitionOptions options;
        options.GetOnlineConfig().loadPatchThreadNum = 3;
        PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
        patchLoader.Init(partitionData, false, lastLoadVersion, 4, true);
        // test is_inc_consistent_with_rt = false, startLoad = 4
        // itemCount = 4 * 5 (4fields * 5dstSegIds)
        ASSERT_EQ(20u, patchLoader._patchWorkItems.size());

        auto TestSortWorkItemByCost = [&patchLoader](const vector<size_t>& costs) {
            vector<size_t> sortedCosts = costs;
            sort(sortedCosts.begin(), sortedCosts.end(),
                 [](const size_t& lhs, const size_t& rhs) { return lhs > rhs; });

            for (size_t i = 0; i < costs.size(); ++i) {
                patchLoader._patchWorkItems[i]->SetCost(costs[i]);
            }
            patchLoader.SortPatchWorkItemsByCost(&(patchLoader._patchWorkItems));

            for (size_t i = 0; i < costs.size(); ++i) {
                EXPECT_EQ(sortedCosts[i], patchLoader._patchWorkItems[i]->GetCost());
            }
        };

        TestSortWorkItemByCost({0, 0, 0, 1, 2, 3, 4, 9, 9, 9, 0, 0, 0, 3, 3, 15, 19, 300, 100, 600});

        TestSortWorkItemByCost({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

        TestSortWorkItemByCost({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
    }
}

void PatchLoaderTest::TestMultiThreadLoadPatch()
{
    // segment_0
    // segment_1 : 1_0.patch
    // segment_2 : 2_0.patch 2_1.patch
    // segment_3 : 3_0.patch 3_1.patch 3_2.patch
    // segment_4 : 4_0.patch 4_1.patch 4_2.patch 4_3.patch
    // segment_5 : 5_0.patch 5_1.patch 5_2.patch 5_3.patch 5_4.patch
    Version version(1);
    for (segmentid_t segId = 0; segId <= 5; segId++) {
        PrepareSegment(segId);
        version.AddSegment(segId);
    }

    Version lastLoadVersion(0);
    for (segmentid_t segId = 0; segId <= 3; segId++) {
        lastLoadVersion.AddSegment(segId);
    }

    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        mRootDir->GetFileSystem(), version, mRootDir->GetLogicalPath(), true, false);

    IndexPartitionOptions options;
    options.GetOnlineConfig().loadPatchThreadNum = 1;
    PatchLoader patchLoader(mSchema, options.GetOnlineConfig());
    patchLoader.Init(partitionData, true, lastLoadVersion, 4, true);

    patchLoader._onlineConfig.loadPatchThreadNum = 3;
    for (size_t i = 0; i < 10u; i++) {
        string id = StringUtil::toString(i);
        size_t patchCount = PatchLoader::PATCH_WORK_ITEM_COST_SAMPLE_COUNT - i;
        PatchWorkItem* item = new SimpleAttrPatchWorkItem(id, patchCount, false, PatchWorkItem::PWIT_VAR_NUM);
        patchLoader._patchWorkItems.push_back(item);
    }
    for (size_t i = 0; i < 10u; i++) {
        string id = StringUtil::toString(10u + i);
        size_t patchCount = PatchLoader::PATCH_WORK_ITEM_COST_SAMPLE_COUNT + i;
        PatchWorkItem* item = new SimpleAttrPatchWorkItem(id, patchCount, false, PatchWorkItem::PWIT_VAR_NUM);
        patchLoader._patchWorkItems.push_back(item);
    }
    for (size_t i = 0; i < 10u; i++) {
        string id = StringUtil::toString(1000u + i);
        size_t patchCount = PatchLoader::PATCH_WORK_ITEM_COST_SAMPLE_COUNT + i;
        PatchWorkItem* item = new SimpleAttrPatchWorkItem(id, patchCount, false, PatchWorkItem::PWIT_VAR_NUM);
        patchLoader._patchWorkItems.push_back(item);
    }

    SubDocModifierPtr modifier(new SubDocModifier(mSchema));
    modifier->mMainModifier.reset(new InplaceModifier(mSchema));
    modifier->mSubModifier.reset(new InplaceModifier(mSchema->GetSubIndexPartitionSchema()));

    patchLoader.Load(lastLoadVersion, modifier);

    for (size_t i = 0; i < 30u; ++i) {
        auto simpleItem = dynamic_cast<SimpleAttrPatchWorkItem*>(patchLoader._patchWorkItems[i]);
        EXPECT_EQ(simpleItem->mCount, simpleItem->_patchCount);
    }
}

void PatchLoaderTest::TestPatchWorkItemProcessMultipleTimes()
{
    SimpleAttrPatchWorkItem* typedItem = new SimpleAttrPatchWorkItem("test", 2019, false, PatchWorkItem::PWIT_VAR_NUM);

    PatchWorkItemPtr item(typedItem);

    item->SetProcessLimit(0);
    item->process();
    EXPECT_EQ(0u, typedItem->mCount);

    item->SetProcessLimit(1);
    item->process();
    EXPECT_EQ(1u, typedItem->mCount);

    item->SetProcessLimit(1);
    item->process();
    EXPECT_EQ(2u, typedItem->mCount);

    item->SetProcessLimit(0);
    item->process();
    EXPECT_EQ(2u, typedItem->mCount);

    item->SetProcessLimit(100);
    item->process();
    EXPECT_EQ(102u, typedItem->mCount);

    item->SetProcessLimit(3000);
    item->process();
    EXPECT_EQ(2019, typedItem->mCount);
}
}} // namespace indexlib::partition
