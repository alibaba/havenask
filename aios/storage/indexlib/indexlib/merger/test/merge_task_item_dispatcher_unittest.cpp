#include "indexlib/merger/test/merge_task_item_dispatcher_unittest.h"

#include <fstream>
#include <random>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/mock_segment_directory.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/merge_task_item_creator.h"
#include "indexlib/merger/test/merge_helper.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace autil;
using namespace std;

using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::plugin;

namespace indexlib { namespace merger {

IE_LOG_SETUP(merger, MergeTaskItemDispatcherTest);
typedef MergeTaskItemCreator::MergeItemIdentify MergeItemIdentify;

struct VectorDoubleComp {
    bool operator()(const vector<double>& lst, const vector<double>& rst)
    {
        double lstSum = 0.0;
        double rstSum = 0.0;
        for (uint32_t i = 0; i < lst.size(); ++i) {
            lstSum += lst[i];
        }
        for (uint32_t i = 0; i < rst.size(); ++i) {
            rstSum += rst[i];
        }
        return (lstSum > rstSum);
    }
};

MergeTaskItemDispatcherTest::MergeTaskItemDispatcherTest() {}

MergeTaskItemDispatcherTest::~MergeTaskItemDispatcherTest() {}

void MergeTaskItemDispatcherTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH() + "/";
    mResMgr.reset(new MergeTaskResourceManager());
    RESET_FILE_SYSTEM("", false, FileSystemOptions::Offline());
}

void MergeTaskItemDispatcherTest::CaseTearDown() {}

config::IndexPartitionSchemaPtr MergeTaskItemDispatcherTest::CreateSimpleSchema(bool isSub)
{
    config::IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "text1:text;text2:text;string1:string;long2:uint32:true",
                                     // Index schema
                                     "pack1:pack:text1;expack1:expack:text2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "string1;long2",
                                     // Summary schema
                                     "string1;text1");
    if (isSub) {
        config::IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(subSchema,
                                         // field schema
                                         "sub_text1:text;sub_text2:text;sub_string1:string:true;sub_long2:uint32:true",
                                         // Index schema
                                         "sub_pack1:pack:sub_text1;sub_expack1:expack:sub_text2;"
                                         // Primary key index schema
                                         "sub_pk:primarykey64:sub_string1",
                                         // Attribute schema
                                         "sub_string1;sub_long2",
                                         // Summary schema
                                         "sub_string1;sub_text1");
        schema->SetSubIndexPartitionSchema(subSchema);
    }
    return schema;
}

config::IndexPartitionSchemaPtr MergeTaskItemDispatcherTest::CreateSchema()
{
    config::IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(
        schema,
        // Field schema
        "text1:text;text2:text;string1:string;long1:uint32;string2:string:true;long2:uint32:true",
        // Index schema
        "pack1:pack:text1,text2;txt1:text:text1;txt2:text:text2;string2:string:string2;expack1:expack:text1,text2;"
        "pack2:pack:text1,text2;"
        // Primary key index schema
        "pk:primarykey64:string1",
        // Attribute schema
        "long1;string1;long2;string2",
        // Summary schema
        "string1;text1");
    return schema;
}

void MergeTaskItemDispatcherTest::TestSimpleProcess()
{
    config::IndexPartitionSchemaPtr schema = CreateSchema();
    segmentid_t maxDocCountInSegmentId = 7;
    string dirName = util::PathUtil::JoinPath(
        mRootDir, "segment_" + autil::StringUtil::toString(maxDocCountInSegmentId) + "_level_0/" + "index/pack1");
    vector<string> fileNames;
    vector<uint32_t> fileListSize;
    fileNames.push_back("dictionary");
    fileNames.push_back("posting");
    fileListSize.push_back(300);
    fileListSize.push_back(400);
    CreateDirAndFile(dirName, fileNames, fileListSize);

    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,7,9,10", GET_PARTITION_DIRECTORY());
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);

    size_t planCount = 3;
    MergeConfig mergeConfig;

    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }
    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator taskItemCreator(&meta, segDir, SegmentDirectoryPtr(), schema, PluginManagerPtr(), mergeConfig,
                                         mResMgr);

    MergeTaskItems allItems = taskItemCreator.CreateMergeTaskItems();
    vector<MergeItemIdentify> mergeItemIdentifys = taskItemCreator.mMergeItemIdentifys;
    INDEXLIB_TEST_EQUAL(size_t(13), mergeItemIdentifys.size());
    INDEXLIB_TEST_EQUAL(mergeItemIdentifys.size() * planCount, allItems.size());

    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());
    uint32_t maxDocCount = 700;

    mergeTaskItemDispatcher.InitMergeTaskItemCost(maxDocCountInSegmentId, maxDocCount, 0, meta, allItems);

    for (size_t i = 0; i < allItems.size(); ++i) {
        uint32_t targetDocCount = (allItems[i].mMergePlanIdx + 1) * 1000;
        if (allItems[i].mMergeType == INDEX_TASK_NAME && allItems[i].mName == "pack1") {
            INDEXLIB_TEST_EQUAL(targetDocCount * 20, allItems[i].mCost);
        } else if (allItems[i].mMergeType == ATTRIBUTE_TASK_NAME && allItems[i].mName == "long1") {
            INDEXLIB_TEST_EQUAL(targetDocCount * 4, allItems[i].mCost);
        } else if (allItems[i].mMergeType == DELETION_MAP_TASK_NAME) {
            ASSERT_DOUBLE_EQ(targetDocCount * MergeTaskItemDispatcher::DELETION_MAP_COST, allItems[i].mCost);
        } else {
            INDEXLIB_TEST_EQUAL(0, allItems[i].mCost);
        }
    }
}

void MergeTaskItemDispatcherTest::TestSimpleProcessWithTruncate()
{
    config::IndexPartitionSchemaPtr schema = CreateSchema();
    segmentid_t maxDocCountInSegmentId = 7;
    string dirName = util::PathUtil::JoinPath(
        mRootDir, "segment_" + autil::StringUtil::toString(maxDocCountInSegmentId) + "_level_0/" + "index/pack1");
    vector<string> fileNames;
    vector<uint32_t> fileListSize;
    fileNames.push_back("dictionary");
    fileNames.push_back("posting");
    fileListSize.push_back(300);
    fileListSize.push_back(400);
    CreateDirAndFile(dirName, fileNames, fileListSize);
    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,7,9,10", GET_PARTITION_DIRECTORY());
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);
    TruncateOptionConfigPtr truncateConfig = TruncateConfigMaker::MakeConfig("", "", "pack1=long2:long2#DESC:long2::");
    INDEXLIB_TEST_TRUE(truncateConfig != NULL);
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, truncateConfig);
    MergeConfig mergeConfig;

    vector<MergePlan> mergePlans;
    MergePlan mergePlan;
    mergePlan.GetTargetSegmentInfo(0).docCount = 2;
    mergePlans.push_back(mergePlan);
    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator taskItemCreator(&meta, segDir, SegmentDirectoryPtr(), schema, PluginManagerPtr(), mergeConfig,
                                         mResMgr);
    MergeTaskItems allItems = taskItemCreator.CreateMergeTaskItems();
    vector<MergeItemIdentify> mergeItemIdentifys = taskItemCreator.mMergeItemIdentifys;
    INDEXLIB_TEST_EQUAL(size_t(13), mergeItemIdentifys.size());

    uint32_t maxDocCount = 700;

    mergeTaskItemDispatcher.InitMergeTaskItemCost(maxDocCountInSegmentId, maxDocCount, 0, meta, allItems);
    size_t i = 0;
    for (; i < allItems.size(); i++) {
        if (allItems[i].mName == "pack1") {
            break;
        }
    }
    INDEXLIB_TEST_EQUAL(40 * mergePlan.GetTargetSegmentInfo(0).docCount, allItems[i].mCost);
}

void MergeTaskItemDispatcherTest::TestSimpleProcessWithSubDoc()
{
    config::IndexPartitionSchemaPtr schema = CreateSimpleSchema(true);
    segmentid_t maxDocCountInSegmentId = 7;
    string dirName = util::PathUtil::JoinPath(
        mRootDir, "segment_" + autil::StringUtil::toString(maxDocCountInSegmentId) + "_level_0/index/pack1");
    string subDirName =
        util::PathUtil::JoinPath(mRootDir, "segment_" + autil::StringUtil::toString(maxDocCountInSegmentId) +
                                               "_level_0/sub_segment/index/sub_pack1");
    vector<string> fileNames;
    vector<uint32_t> fileListSize;

    fileNames.push_back("dictionary");
    fileNames.push_back("posting");
    fileListSize.push_back(300);
    fileListSize.push_back(400);
    CreateDirAndFile(dirName, fileNames, fileListSize);
    CreateDirAndFile(subDirName, fileNames, fileListSize);

    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,7,9,10", GET_PARTITION_DIRECTORY(), true);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(true, false);
    merger::SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, subSegDir, schema, TruncateOptionConfigPtr());
    MergeConfig mergeConfig;

    size_t planCount = 3;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlan.GetSubTargetSegmentInfo(0).docCount = (i + 1) * 500;
        mergePlans.push_back(mergePlan);
    }
    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator taskItemCreator(&meta, segDir, subSegDir, schema, PluginManagerPtr(), mergeConfig, mResMgr);
    MergeTaskItems allItems = taskItemCreator.CreateMergeTaskItems();
    vector<MergeItemIdentify> mergeItemIdentifys = taskItemCreator.mMergeItemIdentifys;
    INDEXLIB_TEST_EQUAL(size_t(14), mergeItemIdentifys.size());
    uint32_t mainDocCount = 700;
    uint32_t subDocCount = 500;
    // set random dataRatio
    for (size_t i = 0; i < allItems.size(); ++i) {
        float dataRatio = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
        if (dataRatio == 0) {
            // avoid invalid data ratio
            dataRatio = 0.1;
        }
        allItems[i].GetParallelMergeItem().SetDataRatio(dataRatio);
    }
    mergeTaskItemDispatcher.InitMergeTaskItemCost(maxDocCountInSegmentId, mainDocCount, subDocCount, meta, allItems);
    INDEXLIB_TEST_EQUAL(mergeItemIdentifys.size() * planCount, allItems.size());

    for (size_t i = 0; i < allItems.size(); ++i) {
        uint32_t targetDocCount = (allItems[i].mMergePlanIdx + 1) * 1000;
        uint32_t subTargetDocCount = (allItems[i].mMergePlanIdx + 1) * 500;
        float dataRatio = allItems[i].GetParallelMergeItem().GetDataRatio();

        if (allItems[i].mMergeType == INDEX_TASK_NAME && allItems[i].mName == "pack1") {
            INDEXLIB_TEST_EQUAL(targetDocCount * double(700) / 700 * 20 * dataRatio, allItems[i].mCost);
        } else if (allItems[i].mMergeType == INDEX_TASK_NAME && allItems[i].mName == "sub_pack1") {
            INDEXLIB_TEST_EQUAL(subTargetDocCount * double(700) / 500 * 20 * dataRatio, allItems[i].mCost);
        } else if (allItems[i].mMergeType == ATTRIBUTE_TASK_NAME && allItems[i].mName == "long1") {
            INDEXLIB_TEST_EQUAL(targetDocCount * 4 * dataRatio, allItems[i].mCost);
        } else if (allItems[i].mMergeType == SUMMARY_TASK_NAME) {
            INDEXLIB_TEST_EQUAL(0, allItems[i].mCost);
        } else if (allItems[i].mMergeType == DELETION_MAP_TASK_NAME) {
            uint32_t docCount = allItems[i].mIsSubItem ? subTargetDocCount : targetDocCount;

            ASSERT_DOUBLE_EQ(docCount * MergeTaskItemDispatcher::DELETION_MAP_COST * dataRatio, allItems[i].mCost);
        } else {
            INDEXLIB_TEST_EQUAL(0, allItems[i].mCost);
        }
    }
}

void MergeTaskItemDispatcherTest::TestDispatchMergeTaskItem()
{
    config::IndexPartitionSchemaPtr schema = CreateSimpleSchema(false);
    string fileStr = "index/pack1 100;"
                     "index/expack1 1100;"
                     "index/pk 50;"
                     "attribute/string1 400;"
                     "attribute/long2 60;"
                     "summary 1000;";
    string segmentRoot = util::PathUtil::JoinPath(mRootDir, "segment_10_level_0");
    MakeFileBySize(segmentRoot, fileStr);
    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,7,9,10", GET_PARTITION_DIRECTORY());
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);

    MergeTaskItemDispatcher dispatcher(segDir, schema, TruncateOptionConfigPtr());
    uint32_t planCount = 2;
    std::vector<MergePlan> mergePlans;
    uint32_t segIdx = 0;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;

        uint32_t segCount = i + 2;
        for (size_t j = 0; j < segCount; ++j) {
            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = version[segIdx++];
            segMergeInfo.segmentInfo.docCount = segMergeInfo.segmentId * 100;
            mergePlan.AddSegment(segMergeInfo);
        }
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 200;
        mergePlans.push_back(mergePlan);
    }

    uint32_t instanceCount = 3;
    MergeConfig mergeConfig;

    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator taskItemCreator(&meta, segDir, SegmentDirectoryPtr(), schema, PluginManagerPtr(), mergeConfig,
                                         mResMgr);
    MergeTaskItems allItems = taskItemCreator.CreateMergeTaskItems();

    std::vector<MergeTaskItems> mergeTaskItemsVec = dispatcher.DispatchMergeTaskItem(meta, allItems, instanceCount);

    INDEXLIB_TEST_EQUAL(instanceCount, mergeTaskItemsVec.size());
    INDEXLIB_TEST_EQUAL(size_t(12), mergeTaskItemsVec[0].size());
    INDEXLIB_TEST_EQUAL(size_t(1), mergeTaskItemsVec[1].size());
    INDEXLIB_TEST_EQUAL(size_t(1), mergeTaskItemsVec[2].size());

    EXPECT_EQ("MergePlan[1] index [pack1] TargetSegmentIdx[-1]", mergeTaskItemsVec[0][0].ToString());
    EXPECT_EQ("MergePlan[0] index [pack1] TargetSegmentIdx[-1]", mergeTaskItemsVec[0][1].ToString());
    EXPECT_EQ("MergePlan[1] summary [__DEFAULT_SUMMARYGROUPNAME__] TargetSegmentIdx[0]",
              mergeTaskItemsVec[0][2].ToString());
    EXPECT_EQ("MergePlan[0] summary [__DEFAULT_SUMMARYGROUPNAME__] TargetSegmentIdx[0]",
              mergeTaskItemsVec[0][3].ToString());
    EXPECT_EQ("MergePlan[1] attribute [string1] TargetSegmentIdx[0]", mergeTaskItemsVec[0][4].ToString());
    EXPECT_EQ("MergePlan[0] attribute [string1] TargetSegmentIdx[0]", mergeTaskItemsVec[0][5].ToString());
    EXPECT_EQ("MergePlan[1] attribute [long2] TargetSegmentIdx[0]", mergeTaskItemsVec[0][6].ToString());
    EXPECT_EQ("MergePlan[1] index [pk] TargetSegmentIdx[-1]", mergeTaskItemsVec[0][7].ToString());
    EXPECT_EQ("MergePlan[0] attribute [long2] TargetSegmentIdx[0]", mergeTaskItemsVec[0][8].ToString());
    EXPECT_EQ("MergePlan[0] index [pk] TargetSegmentIdx[-1]", mergeTaskItemsVec[0][9].ToString());
    EXPECT_EQ("MergePlan[1] deletionmap [] TargetSegmentIdx[-1]", mergeTaskItemsVec[0][10].ToString());
    EXPECT_EQ("MergePlan[0] deletionmap [] TargetSegmentIdx[-1]", mergeTaskItemsVec[0][11].ToString());
    EXPECT_EQ("MergePlan[0] index [expack1] TargetSegmentIdx[-1]", mergeTaskItemsVec[1][0].ToString());
    EXPECT_EQ("MergePlan[1] index [expack1] TargetSegmentIdx[-1]", mergeTaskItemsVec[2][0].ToString());
}

void MergeTaskItemDispatcherTest::MakeFileBySize(const string& segRoot, const string& fileStr)
{
    vector<string> files = StringTokenizer::tokenize(StringView(fileStr), ';',
                                                     StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < files.size(); ++i) {
        vector<string> filePatten = StringTokenizer::tokenize(
            StringView(files[i]), ' ', StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        assert(filePatten.size() == 2);
        string dirPath = util::PathUtil::JoinPath(segRoot, filePatten[0]);
        file_system::FslibWrapper::MkDirE(dirPath, true);

        string filePath = util::PathUtil::JoinPath(dirPath, "test_data_file");
        uint64_t fileLength = StringUtil::fromString<uint64_t>(filePatten[1]);
        string str(fileLength, '0');
        ofstream fout(filePath.c_str());
        fout.write(str.data(), str.length());
        fout.close();
    }
    ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountDir(mRootDir, "", "", FSMT_READ_WRITE, true));
}
/**
void MergeTaskItemDispatcherTest::TestDispatchMergeTaskItemWithSubDoc()
{
    config::IndexPartitionSchemaPtr schema;
    schema = CreateSimpleSchema(true);
    Version version = SegmentDirectoryCreator::MakeVersion(
            "1:-1:2,3,7,9,10", mRootDir, true);

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mRootDir, version));
    segDir->Init(true, false);
    merger::SegmentDirectoryPtr subDir = segDir->GetSubSegmentDirectory();

    MergeTaskItemDispatcher dispatcher(segDir, subDir, schema, TruncateOptionConfigPtr());
    vector<MergePlan> mergePlans;
    uint32_t planCount = 2;
    uint32_t segIdx = 0;
    for(size_t i = 0; i < planCount; i++)
    {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 200;
        mergePlan.subTargetSegmentInfo.docCount = (i + 1) * 300;
        uint32_t segCount = i + 2;
        for(size_t j = 0; j < segCount; j++)
        {
            SegmentMergeInfo segMergeInfo;
            SegmentMergeInfo subSegMergeInfo;
            segMergeInfo.segmentId = version[segIdx];
            segMergeInfo.segmentInfo.docCount = segMergeInfo.segmentId * 100;
            subSegMergeInfo.segmentId = version[segIdx];
            subSegMergeInfo.segmentInfo.docCount = subSegMergeInfo.segmentId * 100;
            mergePlan.inPlanSegMergeInfos.push_back(segMergeInfo);
            mergePlan.subInPlanSegMergeInfos.push_back(subSegMergeInfo);
            segIdx++;
        }
        mergePlans.push_back(mergePlan);
    }
    string fileStr = "index/pack1 100;"
                     "index/expack1 1100;"
                     "index/pk 50;"
                     "attribute/string1 400;"
                     "attribute/long2 60;"
                     "summary 1000;";
    string subFileStr = "index/sub_pack1 100;"
                        "index/sub_expack1 1100;"
                        "index/sub_pk 50;"
                        "attribute/sub_string1 400;"
                        "attribute/sub_long2 60;"
                        "summary 1000;";

    string segmentRoot = util::PathUtil::JoinPath(mRootDir, "segment_10_level_0");
    string subSegmentRoot = util::PathUtil::JoinPath(segmentRoot,
            SUB_SEGMENT_DIR_NAME);

    MakeFileBySize(segmentRoot, fileStr);
    MakeFileBySize(subSegmentRoot, subFileStr);

    uint32_t instanceCount = 3;
    vector<MergeTaskItems> mergeTaskItemsVec = dispatcher.DispatchMergeTaskItem(
            mergePlans, instanceCount);

    INDEXLIB_TEST_EQUAL(instanceCount, mergeTaskItemsVec.size());
    INDEXLIB_TEST_EQUAL(size_t(11), mergeTaskItemsVec[0].size());
    INDEXLIB_TEST_EQUAL(size_t(8), mergeTaskItemsVec[1].size());
    INDEXLIB_TEST_EQUAL(size_t(9), mergeTaskItemsVec[2].size());

    INDEXLIB_TEST_EQUAL("MergePlan[1] index [expack1]", mergeTaskItemsVec[0][0].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] summary [__DEFAULT_SUMMARYGROUPNAME__]", mergeTaskItemsVec[0][1].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] attribute [sub_string1]", mergeTaskItemsVec[0][2].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] attribute [string1]", mergeTaskItemsVec[0][3].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] index [sub_pack1]", mergeTaskItemsVec[0][4].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] index [sub_pack1]", mergeTaskItemsVec[0][5].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] index [pack1]", mergeTaskItemsVec[0][6].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] deletionmap []", mergeTaskItemsVec[0][7].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] deletionmap []", mergeTaskItemsVec[0][8].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] deletionmap []", mergeTaskItemsVec[0][9].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] deletionmap []", mergeTaskItemsVec[0][10].ToString());

    INDEXLIB_TEST_EQUAL("MergePlan[1] summary [__DEFAULT_SUMMARYGROUPNAME__]", mergeTaskItemsVec[1][0].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] index [sub_expack1]", mergeTaskItemsVec[1][1].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] index [expack1]", mergeTaskItemsVec[1][2].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] attribute [sub_string1]", mergeTaskItemsVec[1][3].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] attribute [sub_long2]", mergeTaskItemsVec[1][4].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] attribute [long2]", mergeTaskItemsVec[1][5].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] attribute [sub_long2]", mergeTaskItemsVec[1][6].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] index [pk]", mergeTaskItemsVec[1][7].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] index [sub_expack1]", mergeTaskItemsVec[2][0].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] summary [__DEFAULT_SUMMARYGROUPNAME__]", mergeTaskItemsVec[2][1].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] summary [__DEFAULT_SUMMARYGROUPNAME__]", mergeTaskItemsVec[2][2].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] attribute [string1]", mergeTaskItemsVec[2][3].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] index [pack1]", mergeTaskItemsVec[2][4].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] index [sub_pk]", mergeTaskItemsVec[2][5].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[1] index [pk]", mergeTaskItemsVec[2][6].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] index [sub_pk]", mergeTaskItemsVec[2][7].ToString());
    INDEXLIB_TEST_EQUAL("MergePlan[0] attribute [long2]", mergeTaskItemsVec[2][8].ToString());
}
**/
void MergeTaskItemDispatcherTest::TestGetDirSize()
{
    string phrasePath = "index/phrase/";
    vector<string> pathFileNames;
    vector<uint32_t> fileListSize;
    pathFileNames.push_back("bitmap_dictionary");
    fileListSize.push_back(1024);
    pathFileNames.push_back("bitmap_posting");
    fileListSize.push_back(366);
    pathFileNames.push_back("dictionary");
    fileListSize.push_back(568);
    pathFileNames.push_back("posting");
    fileListSize.push_back(768);

    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,10", GET_PARTITION_DIRECTORY());

    segmentid_t segId = 2;
    string segmentName = "segment_" + autil::StringUtil::toString(segId) + "_level_0";
    string segmentPath = util::PathUtil::JoinPath(mRootDir, segmentName);
    string phraseIndexPath = util::PathUtil::JoinPath(segmentPath, phrasePath);
    CreateDirAndFile(phraseIndexPath, pathFileNames, fileListSize);

    RESET_FILE_SYSTEM();
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);

    config::IndexPartitionSchemaPtr schema;
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());

    uint32_t allSize = 0;
    for (size_t i = 0; i < fileListSize.size(); ++i) {
        allSize += fileListSize[i];
    }

    file_system::DirectoryPtr segDirectory =
        segDir->GetPartitionData()->GetRootDirectory()->GetDirectory(segmentName, true);
    INDEXLIB_TEST_EQUAL(allSize, mergeTaskItemDispatcher.GetDirSize(segDirectory, "index", "phrase"));
}

void MergeTaskItemDispatcherTest::TestGetDirSizeInPackageFile()
{
    vector<string> innerFilePath;
    vector<uint32_t> fileListSize;
    innerFilePath.push_back("index/phrase/bitmap_dictionary");
    fileListSize.push_back(1024);
    innerFilePath.push_back("index/phrase/bitmap_posting");
    fileListSize.push_back(366);
    innerFilePath.push_back("index/phrase/dictionary");
    fileListSize.push_back(568);
    innerFilePath.push_back("index/phrase/posting");
    fileListSize.push_back(768);

    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,10", GET_PARTITION_DIRECTORY());

    segmentid_t segId = 2;
    string segmentName = "segment_" + autil::StringUtil::toString(segId) + "_level_0";
    CreateDirAndFileInPackageFile(segmentName, innerFilePath, fileListSize);

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);

    config::IndexPartitionSchemaPtr schema;
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());

    uint32_t allSize = 0;
    for (size_t i = 0; i < fileListSize.size(); ++i) {
        allSize += fileListSize[i];
    }

    file_system::DirectoryPtr segDirectory =
        segDir->GetPartitionData()->GetRootDirectory()->GetDirectory(segmentName, true);
    INDEXLIB_TEST_EQUAL(allSize, mergeTaskItemDispatcher.GetDirSize(segDirectory, "index", "phrase"));
}

void MergeTaskItemDispatcherTest::TestGetDirSizeWithSubDir()
{
    string pkPath = "index/pk/";
    vector<string> pkPathFileNames;
    vector<uint32_t> pkFileListSize;
    pkPathFileNames.push_back("data");
    pkFileListSize.push_back(1024);

    string pkAttrPath = "index/pk/attribute_pk";
    vector<string> pkAttrPathFileNames;
    vector<uint32_t> pkAttrFileListSize;
    pkAttrPathFileNames.push_back("data");
    pkAttrFileListSize.push_back(2048);

    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,10", GET_PARTITION_DIRECTORY());

    segmentid_t segId = 2;
    string segmentName = "segment_" + autil::StringUtil::toString(segId) + "_level_0";
    string segmentPath = util::PathUtil::JoinPath(mRootDir, segmentName);

    string pkIndexPath = util::PathUtil::JoinPath(segmentPath, pkPath);
    CreateDirAndFile(pkIndexPath, pkPathFileNames, pkFileListSize);
    string pkAttrIndexPath = util::PathUtil::JoinPath(segmentPath, pkAttrPath);
    CreateDirAndFile(pkAttrIndexPath, pkAttrPathFileNames, pkAttrFileListSize);

    RESET_FILE_SYSTEM();
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);
    config::IndexPartitionSchemaPtr schema;
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());
    file_system::DirectoryPtr segDirectory =
        segDir->GetPartitionData()->GetRootDirectory()->GetDirectory(segmentName, true);
    INDEXLIB_TEST_EQUAL((size_t)(3072), mergeTaskItemDispatcher.GetDirSize(segDirectory, "index", "pk"));
}

void MergeTaskItemDispatcherTest::TestGetDirSizeWithSubDirInPackageFile()
{
    vector<string> innerFilePath;
    vector<uint32_t> pkFileListSize;
    innerFilePath.push_back("index/pk/data");
    innerFilePath.push_back("index/pk/attribute_pk/data");
    pkFileListSize.push_back(1024);
    pkFileListSize.push_back(2048);

    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,10", GET_PARTITION_DIRECTORY());
    segmentid_t segId = 2;
    string segmentName = "segment_" + autil::StringUtil::toString(segId) + "_level_0";
    CreateDirAndFileInPackageFile(segmentName, innerFilePath, pkFileListSize);

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);

    config::IndexPartitionSchemaPtr schema;
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());

    file_system::DirectoryPtr segDirectory =
        segDir->GetPartitionData()->GetRootDirectory()->GetDirectory(segmentName, true);
    INDEXLIB_TEST_EQUAL((size_t)(3072), mergeTaskItemDispatcher.GetDirSize(segDirectory, "index", "pk"));
}

void MergeTaskItemDispatcherTest::TestGetMaxDocCount()
{
    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,10", GET_PARTITION_DIRECTORY());
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    config::IndexPartitionSchemaPtr schema;
    string segStrs = "0 100 0;1 23 0;2 76 0;3 121 0;4 323 0|"
                     "5 32 0;6 56 0;7 213 0;8 21 0;9 21 0;10 3 0|"
                     "11 13 0;12 78 0;13 90 0;14 999 0;15 1 0;16 33 0";
    vector<string> segStrVec;
    autil::StringUtil::fromString(segStrs, segStrVec, "|");
    vector<MergePlan> mergePlans;
    mergePlans.resize(segStrVec.size());
    for (size_t i = 0; i < segStrVec.size(); ++i) {
        SegmentMergeInfos segMergeInfos;
        MergeHelper::MakeSegmentMergeInfosWithSegId(segStrVec[i], segMergeInfos);
        for (const auto& segMergeInfo : segMergeInfos) {
            mergePlans[i].AddSegment(segMergeInfo);
        }
    }
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());
    pair<int32_t, int32_t> ret = mergeTaskItemDispatcher.GetSampleSegmentIdx(mergePlans);
    INDEXLIB_TEST_EQUAL(int32_t(2), ret.first);
    INDEXLIB_TEST_EQUAL(int32_t(3), ret.second);
}

void MergeTaskItemDispatcherTest::TestDispatchWorkItems()
{
    double itemsWeights[] = {78, 99, 0.01, 44.9, 2.3, 2.1, 11.5, 88, 33, 0.0};
    MergeTaskItems allItems;
    for (size_t i = 0; i < sizeof(itemsWeights) / sizeof(double); ++i) {
        MergeTaskItem item;
        item.mCost = itemsWeights[i];
        allItems.push_back(item);
    }

    merger::SegmentDirectoryPtr segDir;
    config::IndexPartitionSchemaPtr schema;
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());
    vector<MergeTaskItems> mergedItems;
    size_t instanceCount = 4;
    mergedItems = mergeTaskItemDispatcher.DispatchWorkItems(allItems, instanceCount);
    INDEXLIB_TEST_EQUAL(size_t(instanceCount), mergedItems.size());
    vector<vector<double>> mergedItemsWeight;
    mergedItemsWeight.resize(mergedItems.size());
    for (size_t i = 0; i < mergedItems.size(); ++i) {
        mergedItemsWeight[i].resize(mergedItems[i].size());
        for (size_t j = 0; j < mergedItems[i].size(); ++j) {
            mergedItemsWeight[i][j] = mergedItems[i][j].mCost;
        }
    }
    string expectResult = "78,2.3,2.1,0.01,0;"
                          "88;"
                          "99;"
                          "44.9,33,11.5";

    vector<vector<double>> resultList;
    autil::StringUtil::fromString(expectResult, resultList, ",", ";");
    sort(resultList.begin(), resultList.end(), VectorDoubleComp());
    sort(mergedItemsWeight.begin(), mergedItemsWeight.end(), VectorDoubleComp());
    ASSERT_EQ(resultList.size(), mergedItemsWeight.size());
    for (size_t i = 0; i < mergedItemsWeight.size(); ++i) {
        ASSERT_EQ(resultList[i].size(), mergedItemsWeight[i].size());
        for (size_t j = 0; j < mergedItemsWeight[i].size(); ++j) {
            INDEXLIB_TEST_EQUAL(resultList[i][j], mergedItemsWeight[i][j]);
        }
    }
}

void MergeTaskItemDispatcherTest::TestInitMergeTaskIdentifySubSchema()
{
    config::IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(subSchema,
                                     // field schema
                                     "sub_text1:text;sub_text2:text;sub_string1:string:true;sub_long2:uint32:true",
                                     // Index schema
                                     "sub_pack1:pack:sub_text1;sub_expack1:expack:sub_text2;"
                                     // Primary key index schema
                                     "sub_pk:primarykey64:sub_string1",
                                     // Attribute schema
                                     "sub_string1;sub_long2",
                                     // Summary schema
                                     "sub_string1;sub_text1");
    config::IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "text1:text;text2:text;string1:string;long2:uint32:true",
                                     // Index schema
                                     "pack1:pack:text1;expack1:expack:text2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "string1;long2",
                                     // Summary schema
                                     "string1;text1");
    schema->SetSubIndexPartitionSchema(subSchema);
    Version v;
    merger::SegmentDirectoryPtr segDir(
        new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION)));
    merger::SegmentDirectoryPtr subSegDir(
        new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION)));
    MergeConfig mergeConfig;
    IndexMergeMeta mergeMeta;
    MergeTaskItemCreator taskItemCreator(&mergeMeta, segDir, subSegDir, schema, PluginManagerPtr(), mergeConfig,
                                         mResMgr);
    vector<MergePlan> mergePlans;
    MergePlan mergePlan;
    mergePlan.GetTargetSegmentInfo(0).docCount = 2;
    mergePlans.push_back(mergePlan);
    taskItemCreator.InitMergeTaskIdentify(schema, false, 1, false);
    taskItemCreator.InitMergeTaskIdentify(subSchema, true, 1, false);

    vector<MergeItemIdentify> mergeItemIdentifys = taskItemCreator.mMergeItemIdentifys;
    INDEXLIB_TEST_EQUAL(size_t(14), mergeItemIdentifys.size());
    INDEXLIB_TEST_EQUAL("deletionmap  false", mergeItemIdentifys[0].ToString());
    INDEXLIB_TEST_EQUAL("summary __DEFAULT_SUMMARYGROUPNAME__ false", mergeItemIdentifys[1].ToString());
    INDEXLIB_TEST_EQUAL("index pack1 false", mergeItemIdentifys[2].ToString());
    INDEXLIB_TEST_EQUAL("index expack1 false", mergeItemIdentifys[3].ToString());
    INDEXLIB_TEST_EQUAL("index pk false", mergeItemIdentifys[4].ToString());
    INDEXLIB_TEST_EQUAL("attribute string1 false", mergeItemIdentifys[5].ToString());
    INDEXLIB_TEST_EQUAL("attribute long2 false", mergeItemIdentifys[6].ToString());
    INDEXLIB_TEST_EQUAL("deletionmap  true", mergeItemIdentifys[7].ToString());
    INDEXLIB_TEST_EQUAL("summary __DEFAULT_SUMMARYGROUPNAME__ true", mergeItemIdentifys[8].ToString());
    INDEXLIB_TEST_EQUAL("index sub_pack1 true", mergeItemIdentifys[9].ToString());
    INDEXLIB_TEST_EQUAL("index sub_expack1 true", mergeItemIdentifys[10].ToString());
    INDEXLIB_TEST_EQUAL("index sub_pk true", mergeItemIdentifys[11].ToString());
    INDEXLIB_TEST_EQUAL("attribute sub_string1 true", mergeItemIdentifys[12].ToString());
    INDEXLIB_TEST_EQUAL("attribute sub_long2 true", mergeItemIdentifys[13].ToString());
}

void MergeTaskItemDispatcherTest::TestInitMergeTaskIdentify()
{
    config::IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "text1:text;text2:text;string1:string;long2:uint32:true",
                                     // Index schema
                                     "pack1:pack:text1;expack1:expack:text2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "string1;long2",
                                     // Summary schema
                                     "string1;text1");
    merger::SegmentDirectoryPtr segDir;
    MergeConfig mergeConfig;
    IndexMergeMeta mergeMeta;
    MergeTaskItemCreator taskItemCreator(&mergeMeta, segDir, SegmentDirectoryPtr(), schema, PluginManagerPtr(),
                                         mergeConfig, mResMgr);
    vector<MergePlan> mergePlans;
    MergePlan mergePlan;
    mergePlan.GetTargetSegmentInfo(0).docCount = 2;
    mergePlans.push_back(mergePlan);
    taskItemCreator.InitMergeTaskIdentify(schema, false, 1, false);

    vector<MergeItemIdentify> mergeItemIdentifys = taskItemCreator.mMergeItemIdentifys;
    INDEXLIB_TEST_EQUAL(size_t(7), mergeItemIdentifys.size());
    INDEXLIB_TEST_EQUAL("deletionmap  false", mergeItemIdentifys[0].ToString());
    INDEXLIB_TEST_EQUAL("summary __DEFAULT_SUMMARYGROUPNAME__ false", mergeItemIdentifys[1].ToString());
    INDEXLIB_TEST_EQUAL("index pack1 false", mergeItemIdentifys[2].ToString());
    INDEXLIB_TEST_EQUAL("index expack1 false", mergeItemIdentifys[3].ToString());
    INDEXLIB_TEST_EQUAL("index pk false", mergeItemIdentifys[4].ToString());
    INDEXLIB_TEST_EQUAL("attribute string1 false", mergeItemIdentifys[5].ToString());
    INDEXLIB_TEST_EQUAL("attribute long2 false", mergeItemIdentifys[6].ToString());
}

void MergeTaskItemDispatcherTest::TestSimpleProcessWithMultiShardingIndex()
{
    config::IndexPartitionSchemaPtr schema =
        test::SchemaMaker::MakeSchema(string("text1:text;string1:string;"),
                                      string("pack1:pack:text1:false:2;strindex:string:string1:false:2"), "", "");
    MakeDataForDir(util::PathUtil::JoinPath(mRootDir, "segment_1_level_0/index/pack1_@_0"), "dictionary:20,posting:10");

    MakeDataForDir(util::PathUtil::JoinPath(mRootDir, "segment_1_level_0/index/pack1_@_1"), "dictionary:30,posting:20");

    MakeDataForDir(util::PathUtil::JoinPath(mRootDir, "segment_1_level_0/index/strindex_@_0"),
                   "dictionary:10,posting:40");

    MakeDataForDir(util::PathUtil::JoinPath(mRootDir, "segment_1_level_0/index/strindex_@_1"),
                   "dictionary:20,posting:50");
    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:0,1,2,3", GET_PARTITION_DIRECTORY());
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);
    MergeTaskItemDispatcher mergeTaskItemDispatcher(segDir, schema, TruncateOptionConfigPtr());
    MergeConfig mergeConfig;

    vector<MergePlan> mergePlans;
    MergePlan mergePlan;
    mergePlan.GetTargetSegmentInfo(0).docCount = 700;
    mergePlans.push_back(mergePlan);
    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator taskItemCreator(&meta, segDir, SegmentDirectoryPtr(), schema, PluginManagerPtr(), mergeConfig,
                                         mResMgr);
    MergeTaskItems allItems = taskItemCreator.CreateMergeTaskItems();
    vector<MergeItemIdentify> mergeItemIdentifys = taskItemCreator.mMergeItemIdentifys;
    INDEXLIB_TEST_EQUAL(size_t(7), mergeItemIdentifys.size());

    uint32_t docCount = 10;
    segmentid_t sampleSegId = 1;

    mergeTaskItemDispatcher.InitMergeTaskItemCost(sampleSegId, docCount, 0, meta, allItems);

    double boost = MergeTaskItemDispatcher::INDEX_COST_BOOST;
    CheckMergeTaskItemCost(allItems, "pack1_@_0",
                           (20 + 10) / docCount * boost * mergePlan.GetTargetSegmentInfo(0).docCount);
    CheckMergeTaskItemCost(allItems, "pack1_@_1",
                           (30 + 20) / docCount * boost * mergePlan.GetTargetSegmentInfo(0).docCount);
    CheckMergeTaskItemCost(allItems, "strindex_@_0",
                           (10 + 40) / docCount * boost * mergePlan.GetTargetSegmentInfo(0).docCount);
    CheckMergeTaskItemCost(allItems, "strindex_@_1",
                           (20 + 50) / docCount * boost * mergePlan.GetTargetSegmentInfo(0).docCount);
}

void MergeTaskItemDispatcherTest::MakeDataForDir(const string& dirPath, const string& fileDespStr)
{
    vector<vector<string>> fileInfos;
    StringUtil::fromString(fileDespStr, fileInfos, ":", ",");
    vector<string> fileNames;
    vector<uint32_t> fileSizes;

    for (size_t i = 0; i < fileInfos.size(); ++i) {
        assert(fileInfos[i].size() == 2);
        fileNames.push_back(fileInfos[i][0]);
        fileSizes.push_back(StringUtil::fromString<uint32_t>(fileInfos[i][1]));
    }
    CreateDirAndFile(dirPath, fileNames, fileSizes);
}

void MergeTaskItemDispatcherTest::CheckMergeTaskItemCost(MergeTaskItems& allItems, const string& itemName,
                                                         double expectCost)
{
    bool itemFoundFlag = false;
    int idx = -1;
    for (size_t i = 0; i < allItems.size(); ++i) {
        if (allItems[i].mName == itemName) {
            itemFoundFlag = true;
            idx = i;
            break;
        }
    }
    ASSERT_TRUE(itemFoundFlag);
    ASSERT_EQ(expectCost, allItems[idx].mCost);
}

void MergeTaskItemDispatcherTest::CreateDirAndFile(const string& dirPath, const vector<string>& fileNames,
                                                   vector<uint32_t> fileListSize)
{
    file_system::FslibWrapper::MkDirE(dirPath, true);
    for (size_t i = 0; i < fileNames.size(); ++i) {
        string filePath = util::PathUtil::JoinPath(dirPath, fileNames[i]);
        BufferedFileWriter bufferedFileWriter;
        ASSERT_EQ(file_system::FSEC_OK, bufferedFileWriter.Open(filePath, filePath));
        uint8_t* buf = new uint8_t[fileListSize[i]];
        memset(buf, 0, fileListSize[i]);
        bufferedFileWriter.Write(buf, fileListSize[i]).GetOrThrow();
        ASSERT_EQ(FSEC_OK, bufferedFileWriter.Close());
        delete[] buf;
    }
}

void MergeTaskItemDispatcherTest::CreateDirAndFileInPackageFile(const std::string& segmentPath,
                                                                const std::vector<std::string>& innerFilePath,
                                                                std::vector<uint32_t> fileListSize)
{
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_PACKAGE_MEM;
    auto fs = FileSystemCreator::CreateForWrite("tmp", mRootDir, fsOptions).GetOrThrow();

    // TODO: @qingran remove PackageFileWriterPtr
    file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    file_system::DirectoryPtr segDir = rootDirectory->MakeDirectory(segmentPath, DirectoryOption::Package());

    assert(innerFilePath.size() == fileListSize.size());
    for (size_t i = 0; i < innerFilePath.size(); i++) {
        segDir->MakeDirectory(util::PathUtil::GetParentDirPath(innerFilePath[i]));
        file_system::FileWriterPtr fileWriter = segDir->CreateFileWriter(innerFilePath[i]);

        vector<char> data(fileListSize[i], char(i));
        fileWriter->Write(data.data(), data.size()).GetOrThrow();

        ASSERT_EQ(file_system::FSEC_OK, fileWriter->Close());
    }
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0));
    ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountDir(mRootDir, "", "", FSMT_READ_WRITE, true));
}
}} // namespace indexlib::merger
