#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/test/document_checker_for_gtest.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_meta_creator.h"
#include "indexlib/merger/merge_strategy/realtime_merge_strategy.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/split_strategy/test_split_strategy.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/merger/test/mock_index_partition_merger.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/test/index_partition_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::merger;
using namespace indexlib::test;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace merger {

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0;token1;token2"

// mock to save merge task for check data.
class IndexPartitionMergerToGetMergeTask : public IndexPartitionMerger
{
public:
    IndexPartitionMergerToGetMergeTask(const SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                                       const IndexPartitionOptions& options)
        : IndexPartitionMerger(segDir, schema, options, merger::DumpStrategyPtr(), NULL, plugin::PluginManagerPtr(),
                               index_base::CommonBranchHinterOption::Test())
    {
    }
    ~IndexPartitionMergerToGetMergeTask() {}

public:
    MergeTask GetMergeTask() const { return mMergeTask; }

protected:
    MergeTask CreateMergeTaskByStrategy(bool optimize, const config::MergeConfig& mergeConfig,
                                        const SegmentMergeInfos& segMergeInfos,
                                        const indexlibv2::framework::LevelInfo& levelInfo) const override
    {
        indexlibv2::framework::LevelInfo emptyLevelInfo;
        mMergeTask =
            IndexPartitionMerger::CreateMergeTaskByStrategy(optimize, mergeConfig, segMergeInfos, emptyLevelInfo);
        return mMergeTask;
    }

private:
    mutable MergeTask mMergeTask;
};

class IndexPartitionMergerInteTest : public INDEXLIB_TESTBASE
{
public:
    typedef DocumentMaker::MockIndexPart MockIndexPart;
    typedef DocumentMaker::MockDeletionMap MockDeletionMap;

    typedef IndexTestUtil::DeletionMode DeletionMode;

    typedef vector<pair<uint32_t, DeletionMode>> SegVect;

public:
    IndexPartitionMergerInteTest()
    {
        mSchema.reset(new IndexPartitionSchema);
        index::PartitionSchemaMaker::MakeSchema(
            mSchema,
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

        //"high_frequency_dictionary" : "top10",
        //"high_requency_term_posting_type" : "both",
        IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("pack1");
        indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_both);

        std::shared_ptr<DictionaryConfig> dictConfig = mSchema->AddDictionaryConfig(VOL_NAME, VOL_CONTENT);
        indexConfig->SetDictConfig(dictConfig);

        AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig("long2");
        EXPECT_TRUE(attrConfig->SetCompressType("uniq").IsOK());
        attrConfig->GetFieldConfig()->SetMultiValue(true);

        mAsync = false;
    }

    DECLARE_CLASS_NAME(IndexPartitionMergerInteTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEMP_DATA_PATH();
        mOptions = IndexPartitionOptions(); // reset options.
        mOptions.SetEnablePackageFile(true);
        mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 1;
        mAsync = false;
        mQuotaControl.reset(new QuotaControl(1024 * 1024 * 1024));
        RESET_FILE_SYSTEM("ut", false, file_system::FileSystemOptions::OfflineWithBlockCache(nullptr));
    }

    void CaseTearDown() override { mSchema->SetSubIndexPartitionSchema(config::IndexPartitionSchemaPtr()); }

    config::IndexPartitionSchemaPtr CreateSubSchema()
    {
        config::IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        index::PartitionSchemaMaker::MakeSchema(
            subSchema,
            // field schema
            "sub_text1:text;sub_text2:text;sub_string1:string;sub_long1:uint32;sub_string2:string:true;sub_long2:"
            "uint32:true",
            // Index schema
            "sub_pack1:pack:sub_text1,sub_text2;sub_txt1:text:sub_text1;sub_txt2:text:sub_text2;sub_string2:string:sub_"
            "string2;sub_expack1:expack:sub_text1,sub_text2;sub_pack2:pack:sub_text1,sub_text2;"
            // Primary key index schema
            "sub_pk:primarykey64:sub_string1",
            // Attribute schema
            "sub_long1;sub_string1;sub_long2;sub_string2", "");
        // Summary schema
        //"sub_string1;sub_text1")
        return subSchema;
    }

    void TestCaseForOptimize()
    {
        SegVect segments;
        segments.push_back(make_pair(200, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(300, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(450, IndexTestUtil::DM_NODELETE));

        string mergeTaskStr = "0,1,2";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForMergeFailOverCleanTargetDirs()
    {
        SegVect segments;
        segments.push_back(make_pair(200, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(300, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(450, IndexTestUtil::DM_NODELETE));

        string mergeTaskStr = "";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForMergeWithAsync()
    {
        file_system::FslibWrapper::Reset();
        INDEXLIB_TEST_TRUE(file_system::FslibWrapper::_writePool == NULL);
        INDEXLIB_TEST_TRUE(file_system::FslibWrapper::_readPool == NULL);
        mAsync = true;
        TestCaseForOptimize();
        INDEXLIB_TEST_TRUE(file_system::FslibWrapper::_writePool != NULL);
        // INDEXLIB_TEST_TRUE(file_system::FslibWrapper::mReadPool != NULL);
        file_system::FslibWrapper::Reset();
    }

    void TestCaseForSubDirWithNoDelete()
    {
        SegVect segments;
        SegVect subSegments;
        config::IndexPartitionSchemaPtr subSchema = CreateSubSchema();
        mSchema->SetSubIndexPartitionSchema(subSchema);

        segments.push_back(make_pair(200, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(300, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(450, IndexTestUtil::DM_NODELETE));

        subSegments.push_back(make_pair(300, IndexTestUtil::DM_NODELETE));
        subSegments.push_back(make_pair(400, IndexTestUtil::DM_NODELETE));
        subSegments.push_back(make_pair(550, IndexTestUtil::DM_NODELETE));

        string mergeTaskStr = "0,1,2";
        InternalTestMergeWithSub(segments, subSegments, mergeTaskStr);
    }

    void TestCaseForOptimizeWithDeleteEven()
    {
        SegVect segments;
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(300, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(450, IndexTestUtil::DM_DELETE_EVEN));

        string mergeTaskStr = "0,1,2";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForSubDirWithDeleteSome()
    {
        SegVect segments;
        SegVect subSegments;

        config::IndexPartitionSchemaPtr subSchema = CreateSubSchema();
        mSchema->SetSubIndexPartitionSchema(subSchema);

        segments.push_back(make_pair(20, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(30, IndexTestUtil::DM_DELETE_SOME));

        subSegments.push_back(make_pair(30, IndexTestUtil::DM_DELETE_SOME));
        subSegments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_SOME));

        string mergeTaskStr = "0,1";
        InternalTestMergeWithSub(segments, subSegments, mergeTaskStr);
    }

    void TestCaseForSubDirWithDeleteEven()
    {
        SegVect segments;
        SegVect subSegments;

        config::IndexPartitionSchemaPtr subSchema = CreateSubSchema();
        mSchema->SetSubIndexPartitionSchema(subSchema);

        segments.push_back(make_pair(20, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(30, IndexTestUtil::DM_DELETE_EVEN));

        subSegments.push_back(make_pair(30, IndexTestUtil::DM_DELETE_EVEN));
        subSegments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_EVEN));

        string mergeTaskStr = "0,1";
        InternalTestMergeWithSub(segments, subSegments, mergeTaskStr);
    }

    void TestCaseForOptimizeWithDeleteSome()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_SOME));

        string mergeTaskStr = "0,1,2,3";
        InternalTestMerge(segments, mergeTaskStr, 2);
    }

    void TestCaseForSubDirWithDeleteEvenSplitMode()
    {
        SegVect segments;
        SegVect subSegments;

        config::IndexPartitionSchemaPtr subSchema = CreateSubSchema();
        mSchema->SetSubIndexPartitionSchema(subSchema);

        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(300, IndexTestUtil::DM_DELETE_EVEN));

        subSegments.push_back(make_pair(300, IndexTestUtil::DM_DELETE_EVEN));
        subSegments.push_back(make_pair(400, IndexTestUtil::DM_DELETE_EVEN));

        InternalTestMergeWithSub(segments, subSegments);
    }

    void TestCaseForOptimizeWithDeleteMany()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_MANY));

        InternalTestMerge(segments);
    }

    void TestCaseForOptimizeOneSegment()
    {
        SegVect segments;
        segments.push_back(make_pair(1000, IndexTestUtil::DM_NODELETE));

        InternalTestMerge(segments);
    }

    void TestCaseForSubDirWithDeleteAll()
    {
        SegVect segments;
        SegVect subSegments;

        config::IndexPartitionSchemaPtr subSchema = CreateSubSchema();
        mSchema->SetSubIndexPartitionSchema(subSchema);

        segments.push_back(make_pair(100, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_ALL));

        subSegments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_ALL));
        subSegments.push_back(make_pair(300, IndexTestUtil::DM_DELETE_ALL));
        subSegments.push_back(make_pair(250, IndexTestUtil::DM_DELETE_ALL));

        string mergeTaskStr = "0,1,2";
        InternalTestMergeWithSub(segments, subSegments, mergeTaskStr);
    }

    void TestCaseForOptimizeOneSegmentWithDeleteEven()
    {
        SegVect segments;
        segments.push_back(make_pair(1000, IndexTestUtil::DM_DELETE_EVEN));

        string mergeTaskStr = "0";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForOptimizeOneSegmentWithAllDelete()
    {
        SegVect segments;
        segments.push_back(make_pair(1000, IndexTestUtil::DM_DELETE_ALL));

        string mergeTaskStr = "0,1";
        InternalTestMerge(segments, mergeTaskStr, 0);
    }

    void TestCaseForOptimizeWithLastDeleteAll()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_ALL));

        string mergeTaskStr = "0,1,2";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForOptimizeWithFirstDeleteAll()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_EVEN));

        string mergeTaskStr = "0,1,2,3";
        InternalTestMerge(segments, mergeTaskStr, 1);
    }

    void TestCaseForOptimizeWithDeleteAll()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_ALL));

        string mergeTaskStr = "0,1,2";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForOptimizeManySegmentManyDocsLongCaseTest()
    {
        SegVect segments;
        segments.push_back(make_pair(1000, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(200, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(150, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(1500, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(10, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(10, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(500, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));

        string mergeTaskStr = "0,1,2,3,4,5,6,7";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForOptimizeManySegManyDocsWithDelLongCaseTest()
    {
        SegVect segments;
        segments.push_back(make_pair(1000, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(200, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(150, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(1500, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(10, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(10, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(500, IndexTestUtil::DM_DELETE_MANY));
        segments.push_back(make_pair(1, IndexTestUtil::DM_DELETE_MANY));

        string mergeTaskStr = "0,1,2,3,4,5,6,7,8";
        InternalTestMerge(segments, mergeTaskStr, 7);
    }

    void TestCaseForMergeWithPlanAndNoDelete()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(200, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(150, IndexTestUtil::DM_NODELETE));

        string mergeTaskStr = "0,2";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForMergeWithTwoPlanAndNoDelete()
    {
        SegVect segments;
        segments.push_back(make_pair(100, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(200, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(150, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(350, IndexTestUtil::DM_NODELETE));

        string mergeTaskStr = "0,2;1,3";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForMergeWithThreePlanAndSomeDeleteLongCaseTest()
    {
        SegVect segments;
        segments.push_back(make_pair(112, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(232, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(425, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(251, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(25, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(91, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(911, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(511, IndexTestUtil::DM_DELETE_MANY));

        string mergeTaskStr = "0,2;4,5,6;7";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForMergeSubWithThreePlanAndSomeDeleteLongCaseTest()
    {
        SegVect segments;
        config::IndexPartitionSchemaPtr subSchema = CreateSubSchema();
        mSchema->SetSubIndexPartitionSchema(subSchema);
        segments.push_back(make_pair(112, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(232, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(425, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(251, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(25, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(91, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(911, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(511, IndexTestUtil::DM_DELETE_MANY));

        string mergeTaskStr = "0,2;4,5,6;7";
        InternalTestMergeWithSub(segments, segments, mergeTaskStr);
    }

    void TestCaseForMergeWithTwoPlanAndOneMergeAllDelete()
    {
        SegVect segments;
        segments.push_back(make_pair(312, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(22, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(525, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(21, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(205, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(191, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(811, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(111, IndexTestUtil::DM_DELETE_MANY));

        string mergeTaskStr = "0,2;4,5,6";
        InternalTestMerge(segments, mergeTaskStr);
    }

    void TestCaseForMergeLeavingSomeDeletion()
    {
        SegVect segments;
        segments.push_back(make_pair(312, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(22, IndexTestUtil::DM_DELETE_ALL));
        MockIndexPart mockIndexPart;
        MakeIndex(segments, INVALID_SEGMENTID, mockIndexPart);
        {
            OfflinePartition indexPart(IndexPartitionResource::Create(IndexPartitionResource::IR_OFFLINE_LEGACY));
            IndexPartitionOptions options = mOptions;
            options.SetIsOnline(false);
            indexPart.Open(mRootDir, "", mSchema, options);
            PartitionWriterPtr writer = indexPart.GetWriter();
            OfflinePartitionWriterPtr offlineWriter = DYNAMIC_POINTER_CAST(OfflinePartitionWriter, writer);
            writer.reset();
            PartitionModifierPtr modifier = offlineWriter->GetModifier();
            modifier->RemoveDocument(0);
            DocumentMaker::DeleteDocument(mockIndexPart, 0);
            modifier.reset();
            offlineWriter->Close();
        }
        string mergeTaskStr = "1,2;";
        MergeTask mergeTask;
        MergeIndex(mergeTaskStr, mergeTask);

        MockIndexPart newMockIndexPart;
        ReclaimMockIndexWithTask(mockIndexPart, segments, mergeTask, newMockIndexPart);
        CheckIndex(newMockIndexPart, false);
    }

    void TestCaseForPriorityQueueStrategy()
    {
        SegVect segments;
        segments.push_back(make_pair(312, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(22, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(525, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(21, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(205, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(191, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(811, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(111, IndexTestUtil::DM_NODELETE));

        mOptions.GetMergeConfig().mergeStrategyStr = "priority_queue";
        mOptions.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "max-doc-count=8000";
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions = "conflict-segment-count=3";

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 8, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 8);
        INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(2, version[0]);
        INDEXLIB_TEST_EQUAL(6, version[1]);
        INDEXLIB_TEST_EQUAL(8, version[2]);

        {
            ClearRootDir();
            InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
            Version version;
            ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 8, "", FSMT_READ_ONLY, nullptr));
            VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 8);
            INDEXLIB_TEST_EQUAL(size_t(4), version.GetSegmentCount());
            INDEXLIB_TEST_EQUAL(2, version[0]);
            INDEXLIB_TEST_EQUAL(6, version[1]);
            INDEXLIB_TEST_EQUAL(8, version[2]);
            INDEXLIB_TEST_EQUAL(9, version[3]);
        }
    }

    void TestCaseForPriorityQueueStrategyWithDelPercent()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(30, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(45, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(70, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(400, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(500, IndexTestUtil::DM_DELETE_ALL));

        mOptions.GetMergeConfig().mergeStrategyStr = "priority_queue";
        mOptions.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "max-valid-doc-count=150";
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions =
            "conflict-segment-count=5;conflict-delete-percent=45";

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(7, version[1]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(7, version[1]);
        INDEXLIB_TEST_EQUAL(8, version[2]);
    }

    void TestCaseForPriorityQueueStrategyTestLimitMaxMergedSegmentDocCount()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(30, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(45, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(70, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(400, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(500, IndexTestUtil::DM_DELETE_ALL));

        mOptions.GetMergeConfig().mergeStrategyStr = "priority_queue";
        mOptions.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "";
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions = "conflict-segment-count=1";
        mOptions.GetMergeConfig().mergeStrategyParameter.outputLimitParam = "max-merged-segment-doc-count=100";

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(3, version[0]);
        INDEXLIB_TEST_EQUAL(5, version[1]);
        INDEXLIB_TEST_EQUAL(7, version[2]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(4), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(3, version[0]);
        INDEXLIB_TEST_EQUAL(5, version[1]);
        INDEXLIB_TEST_EQUAL(7, version[2]);
        INDEXLIB_TEST_EQUAL(8, version[3]);
    }

    void TestCaseForPriorityQueueStrategyTestLimitMaxTotalMergeDocCount()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(30, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(45, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(70, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(400, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(500, IndexTestUtil::DM_DELETE_ALL));

        mOptions.GetMergeConfig().mergeStrategyStr = "priority_queue";
        mOptions.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "";
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions =
            "conflict-segment-count=1;conflict-delete-percent=40";
        mOptions.GetMergeConfig().mergeStrategyParameter.outputLimitParam =
            "max-merged-segment-doc-count=80;max-total-merge-doc-count=200";

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(7, version[1]);
        INDEXLIB_TEST_EQUAL(8, version[2]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(5), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(7, version[1]);
        INDEXLIB_TEST_EQUAL(8, version[2]);
        INDEXLIB_TEST_EQUAL(9, version[3]);
        INDEXLIB_TEST_EQUAL(10, version[4]);
    }

    void TestCaseForPriorityQueueStrategyWithMaxSegmentSize()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(30, IndexTestUtil::DM_NODELETE));

        mOptions.GetMergeConfig().mergeStrategyStr = "priority_queue";
        mOptions.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "max-segment-size=0";
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions = "conflict-segment-count=1";
        mOptions.GetMergeConfig().mergeStrategyParameter.outputLimitParam = "";

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 1);
        INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(0, version[0]);
        INDEXLIB_TEST_EQUAL(1, version[1]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 1);
        // no merge happened
        INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(0, version[0]);
        INDEXLIB_TEST_EQUAL(1, version[1]);
    }

    void TestCaseForBalanceTreeStrategy()
    {
        SegVect segments;
        segments.push_back(make_pair(312, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(22, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(525, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(21, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(205, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(191, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(811, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(111, IndexTestUtil::DM_NODELETE));

        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=300;max-doc-count=8000;"
                                                                         "conflict-segment-number=3");

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 8, "", FSMT_READ_ONLY, nullptr));
        Version version;
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 8);
        INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(6, version[0]);
        INDEXLIB_TEST_EQUAL(8, version[1]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 8, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 8);
        INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(6, version[0]);
        INDEXLIB_TEST_EQUAL(8, version[1]);
        INDEXLIB_TEST_EQUAL(9, version[2]);
    }

    void TestCaseForBalanceTreeStrategyTestLimit()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(15, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(50, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(45, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(80, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(70, IndexTestUtil::DM_DELETE_SOME));
        segments.push_back(make_pair(99, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(110, IndexTestUtil::DM_DELETE_ALL));

        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=30;max-doc-count=100;"
                                                                         "conflict-segment-number=3");

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 9, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 9);
        INDEXLIB_TEST_EQUAL(size_t(5), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(0, version[0]);
        INDEXLIB_TEST_EQUAL(1, version[1]);
        INDEXLIB_TEST_EQUAL(7, version[2]);
        INDEXLIB_TEST_EQUAL(8, version[3]);
        INDEXLIB_TEST_EQUAL(9, version[4]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 9, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 9);
        INDEXLIB_TEST_EQUAL(size_t(6), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(0, version[0]);
        INDEXLIB_TEST_EQUAL(1, version[1]);
        INDEXLIB_TEST_EQUAL(7, version[2]);
        INDEXLIB_TEST_EQUAL(8, version[3]);
        INDEXLIB_TEST_EQUAL(9, version[4]);
        INDEXLIB_TEST_EQUAL(10, version[5]);
    }
    void TestCaseForBalanceTreeStrategyWithDelPercent()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_DELETE_ALL));
        segments.push_back(make_pair(30, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(40, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(45, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(70, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(400, IndexTestUtil::DM_DELETE_EVEN));
        segments.push_back(make_pair(500, IndexTestUtil::DM_DELETE_ALL));

        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=30;max-doc-count=100;"
                                                                         "conflict-segment-number=3;"
                                                                         "conflict-delete-percent=45;"
                                                                         "max-valid-doc-count=150");

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});

        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(7, version[1]);
        INDEXLIB_TEST_EQUAL(8, version[2]);

        ClearRootDir();
        version.Clear();
        InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 7, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 7);
        INDEXLIB_TEST_EQUAL(size_t(4), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(7, version[1]);
        INDEXLIB_TEST_EQUAL(8, version[2]);
        INDEXLIB_TEST_EQUAL(9, version[3]);
    }

    void TestCaseForBalanceTreeStrategyMerge3Step()
    {
        SegVect segments;
        segments.push_back(make_pair(10, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(15, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(24, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(15, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(45, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(55, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(110, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(100, IndexTestUtil::DM_NODELETE));

        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=30;max-doc-count=1000;"
                                                                         "conflict-segment-number=3");

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1});
        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 8, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 8);
        INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(3, version[0]);
        INDEXLIB_TEST_EQUAL(8, version[1]);

        {
            ClearRootDir();
            InternalTestMerge(segments, "", INVALID_SEGMENTID, {2});
            Version version;
            ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 8, "", FSMT_READ_ONLY, nullptr));
            VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 8);
            INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
            INDEXLIB_TEST_EQUAL(3, version[0]);
            INDEXLIB_TEST_EQUAL(8, version[1]);
            INDEXLIB_TEST_EQUAL(9, version[2]);
        }
    }

    // index, summary, attribute are all empty
    void TestMergeEmptyDocuments()
    {
        // make 3 segments dumped by empty docs
        MakeEmptyDocsAndDump();
        MakeEmptyDocsAndDump();
        MakeEmptyDocsAndDump();

        Version version;
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
        mSegmentDir.reset(new SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
        mSegmentDir->Init(false, true);
        mOptions.SetIsOnline(false);
        IndexPartitionMerger merger(mSegmentDir, mSchema, mOptions, merger::DumpStrategyPtr(), NULL,
                                    plugin::PluginManagerPtr(), CommonBranchHinterOption::Test());
        merger.Merge(true);

        IndexPartitionOptions onlineOptions = mOptions;
        onlineOptions.SetIsOnline(true);
        OnlinePartition onlinePartition;
        ASSERT_NO_THROW(onlinePartition.Open(mRootDir, "", mSchema, onlineOptions));

        {
            SetTargetSegmentCount(2);
            Version version;
            VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
            mSegmentDir.reset(new SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
            mSegmentDir->Init(false, true);
            mOptions.SetIsOnline(false);
            IndexPartitionMerger merger(mSegmentDir, mSchema, mOptions, merger::DumpStrategyPtr(), NULL,
                                        plugin::PluginManagerPtr(), CommonBranchHinterOption::Test());
            merger.Merge(true);

            IndexPartitionOptions onlineOptions = mOptions;
            onlineOptions.SetIsOnline(true);
            OnlinePartition onlinePartition;
            ASSERT_NO_THROW(onlinePartition.Open(mRootDir, "", mSchema, onlineOptions));
        }
    }

    void TestOneSegmentWithNoDel()
    {
        string docStr = "{ 1, "
                        "[text1: (token1^3 token2) (token2^2)],"
                        "[string1: (token8)],"
                        "[string2: (token3)],"
                        "[text2: (token2 token2^9)],"
                        "};"
                        "{ 3, "
                        "[text1: (token5^3 token2^9)],"
                        "[string1: (token2)],"
                        "[string2: (token2) (token4 token8)],"
                        "};";

        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);

        // add document and dump.
        mOptions.GetMergeConfig().SetEnablePackageFile(false);
        IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(indexBuilder.Init());

        for (size_t i = 0; i < docVect.size(); ++i) {
            indexBuilder.Build(docVect[i]);
        }
        indexBuilder.EndIndex();
        indexBuilder.TEST_BranchFSMoveToMainRoad();
        indexBuilder.Merge(mOptions);
        INDEXLIB_TEST_TRUE(!FslibWrapper::IsExist(mRootDir + "segment_0_level_0/").GetOrThrow());
        INDEXLIB_TEST_TRUE(FslibWrapper::IsExist(mRootDir + "segment_1_level_0/").GetOrThrow());
    }

    void TestCaseForTimestampInSegmentInfo()
    {
        IndexPartitionOptions options;
        options.GetMergeConfig().mergeStrategyStr = "realtime";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT + "=3;" + RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD +
            "=160;" + RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD + "=128");

        int64_t segTime1 = autil::TimeUtility::currentTime();
        int64_t segTime2 = segTime1 - 100;
        int64_t segTime3 = segTime1 - 200;

        CreateOneSegmentData(100, options, segTime3);
        CreateOneSegmentData(100, options, segTime2);
        CreateOneSegmentData(100, options, segTime1);

        IndexBuilder builder(mRootDir, options, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(builder.Init());

        builder.Merge(options);
        Version version;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
        INDEXLIB_TEST_EQUAL((versionid_t)3, version.GetVersionId());
        INDEXLIB_TEST_EQUAL((segmentid_t)5, version[0]);

        auto segDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_5_level_0");
        ASSERT_NE(segDir, nullptr);
        SegmentInfo segInfo;
        segInfo.Load(segDir);
        INDEXLIB_TEST_EQUAL(segTime1, segInfo.timestamp);
    }

    void TestCaseForTruncateAndBalanceTreeStrategy()
    {
        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=1;max-doc-count=5;"
                                                                         "conflict-segment-number=2;"
                                                                         "conflict-delete-percent=1;");

        // TODO: check pack with section
        mOptions.GetMergeConfig().truncateOptionConfig =
            TruncateConfigMaker::MakeConfig("0:2", "", "txt1=long1:long1#DESC:::");
        mOptions.GetMergeConfig().mergeThreadCount = 1;

        {
            SegVect segments;
            segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
            segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
            segments.push_back(make_pair(6, IndexTestUtil::DM_DELETE_EVEN));

            InternalTestMerge(segments, "", INVALID_SEGMENTID, {1}, true);

            Version version;
            ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 5, "", FSMT_READ_ONLY, nullptr));
            VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 5);
            INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
            INDEXLIB_TEST_EQUAL(5, version[0]);
            INDEXLIB_TEST_EQUAL(6, version[1]);

            SegmentDirectory segmentDirectory(GET_PARTITION_DIRECTORY(), version);
            auto segDir5 = segmentDirectory.GetSegmentDirectory(5);
            INDEXLIB_TEST_TRUE(segDir5->IsExist("index/txt1_long1"));

            auto segDir6 = segmentDirectory.GetSegmentDirectory(6);
            INDEXLIB_TEST_TRUE(segDir6->IsExist("index/txt1_long1"));
            INDEXLIB_TEST_TRUE(checkTruncateDeployIndex());
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir5));
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir6));
        }

        {
            SegVect segments;
            segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
            segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
            segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));

            // multi output
            ClearRootDir();
            InternalTestMerge(segments, "", INVALID_SEGMENTID, {2}, true);

            Version version;
            ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 5, "", FSMT_READ_ONLY, nullptr));
            VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 5);
            INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
            INDEXLIB_TEST_EQUAL(5, version[0]);
            INDEXLIB_TEST_EQUAL(6, version[1]);
            INDEXLIB_TEST_EQUAL(7, version[2]);

            SegmentDirectory segmentDirectory(GET_PARTITION_DIRECTORY(), version);
            auto segDir5 = segmentDirectory.GetSegmentDirectory(5);
            INDEXLIB_TEST_TRUE(segDir5->IsExist("index/txt1_long1"));
            auto segDir6 = segmentDirectory.GetSegmentDirectory(6);
            INDEXLIB_TEST_TRUE(segDir6->IsExist("index/txt1_long1"));
            auto segDir7 = segmentDirectory.GetSegmentDirectory(7);
            INDEXLIB_TEST_TRUE(segDir7->IsExist("index/txt1_long1"));

            INDEXLIB_TEST_TRUE(checkTruncateDeployIndex());

            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir5));
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir6));
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir7));
        }
    }

    void TestCaseForDeployIndexWithTruncate()
    {
        SegVect segments;
        segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
        segments.push_back(make_pair(6, IndexTestUtil::DM_DELETE_EVEN));

        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=1;max-doc-count=5;"
                                                                         "conflict-segment-number=2;"
                                                                         "conflict-delete-percent=1;");

        // TODO: check pack with section
        mOptions.GetMergeConfig().truncateOptionConfig =
            TruncateConfigMaker::MakeConfig("0:2", "", "txt1=long1:long1#DESC:::");
        mOptions.GetMergeConfig().mergeThreadCount = 1;

        InternalTestMerge(segments, "", INVALID_SEGMENTID, {1}, true);

        Version version;
        ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 5, "", FSMT_READ_ONLY, nullptr));
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 5);

        INDEXLIB_TEST_EQUAL(size_t(2), version.GetSegmentCount());
        INDEXLIB_TEST_EQUAL(5, version[0]);
        INDEXLIB_TEST_EQUAL(6, version[1]);

        SegmentDirectory segmentDirectory(GET_PARTITION_DIRECTORY(), version);
        auto segDir5 = segmentDirectory.GetSegmentDirectory(5);
        INDEXLIB_TEST_TRUE(segDir5->IsExist("index/txt1_long1"));

        auto segDir6 = segmentDirectory.GetSegmentDirectory(6);
        INDEXLIB_TEST_TRUE(segDir6->IsExist("index/txt1_long1"));

        INDEXLIB_TEST_TRUE(checkTruncateDeployIndex());
        INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir5));
        INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir6));

        {
            SegVect segments2;
            segments2.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
            segments2.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));
            segments2.push_back(make_pair(1, IndexTestUtil::DM_NODELETE));

            ClearRootDir();
            Version version;
            InternalTestMerge(segments2, "", INVALID_SEGMENTID, {2}, true);
            ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(GET_TEMP_DATA_PATH(), 5, "", FSMT_READ_ONLY, nullptr));
            VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, 5);
            INDEXLIB_TEST_EQUAL(size_t(3), version.GetSegmentCount());
            INDEXLIB_TEST_EQUAL(5, version[0]);
            INDEXLIB_TEST_EQUAL(6, version[1]);
            INDEXLIB_TEST_EQUAL(7, version[2]);

            SegmentDirectory segmentDirectory(GET_PARTITION_DIRECTORY(), version);
            auto segDir5 = segmentDirectory.GetSegmentDirectory(5);
            INDEXLIB_TEST_TRUE(segDir5->IsExist("index/txt1_long1"));
            auto segDir6 = segmentDirectory.GetSegmentDirectory(6);
            INDEXLIB_TEST_TRUE(segDir6->IsExist("index/txt1_long1"));
            auto segDir7 = segmentDirectory.GetSegmentDirectory(7);
            INDEXLIB_TEST_TRUE(segDir7->IsExist("index/txt1_long1"));

            INDEXLIB_TEST_TRUE(checkTruncateDeployIndex());
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir5));
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir6));
            INDEXLIB_TEST_TRUE(checkSegmentDeployIndex(segDir7));
        }
    }

    bool checkSegmentDeployIndex(const file_system::DirectoryPtr& segDir) { return segDir->IsExist(SEGMENT_FILE_LIST); }
    bool checkTruncateDeployIndex()
    {
        string truncateMetaPath = util::PathUtil::JoinPath(mRootDir, TRUNCATE_META_DIR_NAME);
        string truncateIndex = util::PathUtil::JoinPath(truncateMetaPath, SEGMENT_FILE_LIST);
        if (FslibWrapper::IsExist(truncateIndex).GetOrThrow()) {
            return true;
        }
        return false;
    }

    void TestCaseForNumberIndex()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(
            mSchema,
            // Field schema
            "field1:int8;field2:uint8;field3:int16;field4:uint16;field5:int32;field6:uint32;field7:int64;field8:uint64",
            // Index schema
            "number1:number:field1;number2:number:field2;number3:number:field3;number4:number:field4;number5:number:"
            "field5;"
            "number6:number:field6;number7:number:field7;number8:number:field8"
            // Primary key index schema
            "",
            // Attribute schema
            "field1;field3;field5;field7",
            // Summary schema
            "field2;field4;field6;field8");

        string docStr = "{ 1, "
                        "[field1: ( -1 -10 )],"
                        "[field2: (0 10 250 255)],"
                        "[field3: (-1 -32768 0 5 32767)],"
                        "[field4: (0 20 65530 65535)],"
                        "[field5: (-1 -32769 0 0 15 65536)],"
                        "[field6: (0 20 65535)],"
                        "[field7: (-1 -1000000 20 20 65535)],"
                        "[field8: (0 20 65535 10000000)],"
                        "};"
                        "{ 3, "
                        "[field1: ( -10 )],"
                        "[field2: (0 110 250)],"
                        "[field3: (-32768 0 5 32767 9)],"
                        "[field4: (0 20 65534 888)],"
                        "[field5: (-32769 0  15 65536)],"
                        "[field6: (0 20 65534 123)],"
                        "[field7: (-1000000 20 20 65535)],"
                        "[field8: (0 20 65535 10000000)],"
                        "};";

        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);

        DocumentMaker::DocumentVect tmpDocVect;
        tmpDocVect.push_back(docVect[0]);
        MakeIndexPartition(mRootDir, mSchema, mOptions, tmpDocVect, false);

        tmpDocVect.clear();
        tmpDocVect.push_back(docVect[1]);
        MakeIndexPartition(mRootDir, mSchema, mOptions, tmpDocVect, true);
        INDEXLIB_TEST_TRUE(FslibWrapper::IsExist(mRootDir + "segment_2_level_0/").GetOrThrow());
    }

    void TestCaseForPackIndexWithTfBitmap()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(
            mSchema,
            // Field schema
            "text1:text;text2:text;string1:string;long1:uint32;string2:string:true;long2:uint32:true",
            // Index schema
            "pack2:pack:text1,text2;"
            // Primary key index schema
            "pk:primarykey64:string1",
            // Attribute schema
            "long1;string1;long2;string2",
            // Summary schema
            "string1;text1");

        IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("pack2");
        indexConfig->SetOptionFlag(OPTION_FLAG_ALL | of_tf_bitmap);

        string docStr = "{ 1, "
                        "[text1: (token1^3 token2) (token2^2)],"
                        "[string1: (token1) ],"
                        "[text2: (token2 token2^9)],"
                        "};"
                        "{ 3, "
                        "[text1: (token1 token2^9 token2 token3)],"
                        "[string1: (token2) ],"
                        "};";

        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        INDEXLIB_TEST_TRUE(docVect.size() == 2);

        DocumentMaker::DocumentVect tmpDocVect;
        tmpDocVect.push_back(docVect[0]);
        MakeIndexPartition(mRootDir, mSchema, mOptions, tmpDocVect, false);

        tmpDocVect.clear();
        tmpDocVect.push_back(docVect[1]);
        MakeIndexPartition(mRootDir, mSchema, mOptions, tmpDocVect, true);

        INDEXLIB_TEST_TRUE(FslibWrapper::IsExist(mRootDir + "segment_2_level_0/").GetOrThrow());

        // check position
        OnlinePartition indexPart;
        indexPart.Open(mRootDir, "", mSchema, mOptions);
        IndexPartitionReaderPtr reader = indexPart.GetReader();
        std::shared_ptr<InvertedIndexReader> indexReader = reader->GetInvertedIndexReader("pack2");
        Term term("token2", "pack2");
        std::shared_ptr<PostingIterator> it(indexReader->Lookup(term).ValueOrThrow());
        INDEXLIB_TEST_TRUE(it);
        TermMatchData tmd;
        vector<pos_t> positions;
        docid_t docId = it->SeekDoc(0);
        while (docId != INVALID_DOCID) {
            docId++;
            it->Unpack(tmd);
            std::shared_ptr<InDocPositionIterator> posIt = tmd.GetInDocPositionIterator();
            pos_t pos = posIt->SeekPosition(0);
            while (pos != INVALID_POSITION) {
                positions.push_back(pos);
                pos = posIt->SeekPosition(pos);
            }
            docId = it->SeekDoc(docId);
        }
        pos_t posArray[] = {1, 3, 5, 6, 1, 2};
        vector<pos_t> expectedPositions(posArray, posArray + sizeof(posArray) / sizeof(pos_t));
        INDEXLIB_TEST_EQUAL(expectedPositions.size(), positions.size());
        for (size_t i = 0; i < expectedPositions.size(); i++) {
            INDEXLIB_TEST_EQUAL(expectedPositions[i], positions[i]);
        }
    }

    void TestCaseForVersionAfterMerge()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(
            mSchema,
            // Field schema
            "field1:int8;field2:uint8;field3:int16;field4:uint16;field5:int32;field6:uint32;field7:int64;field8:uint64",
            // Index schema
            "number1:number:field1;number2:number:field2;number3:number:field3;number4:number:field4;number5:number:"
            "field5;"
            "number6:number:field6;number7:number:field7;number8:number:field8"
            // Primary key index schema
            "",
            // Attribute schema
            "field1;field3;field5;field7",
            // Summary schema
            "field2;field4;field6;field8");

        string docStr = "{ 1, "
                        "[field1: ( -1 -10 )],"
                        "[field2: (0 10 250 255)],"
                        "[field3: (-1 -32768 0 5 32767)],"
                        "[field4: (0 20 65530 65535)],"
                        "[field5: (-1 -32769 0 0 15 65536)],"
                        "[field6: (0 20 65535)],"
                        "[field7: (-1 -1000000 20 20 65535)],"
                        "[field8: (0 20 65535 10000000)],"
                        "};"
                        "{ 3, "
                        "[field1: ( -10 )],"
                        "[field2: (0 110 250)],"
                        "[field3: (-32768 0 5 32767 9)],"
                        "[field4: (0 20 65534 888)],"
                        "[field5: (-32769 0  15 65536)],"
                        "[field6: (0 20 65534 123)],"
                        "[field7: (-1000000 20 20 65535)],"
                        "[field8: (0 20 65535 10000000)],"
                        "};";

        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        int64_t startTimestamp = 100;
        SetTimestamp(docVect, startTimestamp);

        // add document and dump.
        IndexBuilderPtr builder(
            new IndexBuilder(mRootDir, mOptions, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test()));
        INDEXLIB_TEST_TRUE(builder->Init());

        builder->Build(docVect[0]);
        builder->EndIndex();
        builder->TEST_BranchFSMoveToMainRoad();

        Version version0;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version0, INVALID_VERSIONID);
        INDEXLIB_TEST_EQUAL((versionid_t)0, version0.GetVersionId());
        INDEXLIB_TEST_EQUAL(startTimestamp, version0.GetTimestamp());

        // dump segment1
        builder.reset(
            new IndexBuilder(mRootDir, mOptions, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test()));
        INDEXLIB_TEST_TRUE(builder->Init());

        builder->Build(docVect[1]);
        builder->EndIndex();
        builder->TEST_BranchFSMoveToMainRoad();

        Version version1;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version1, INVALID_VERSIONID);
        INDEXLIB_TEST_EQUAL((versionid_t)1, version1.GetVersionId());
        INDEXLIB_TEST_EQUAL(startTimestamp + 1, version1.GetTimestamp());

        // merge
        builder->Merge(mOptions);
        Version version2;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version2, INVALID_VERSIONID);
        INDEXLIB_TEST_EQUAL((versionid_t)2, version2.GetVersionId());
        INDEXLIB_TEST_EQUAL(startTimestamp + 1, version2.GetTimestamp());
    }

    void TestCaseForVersionAfterMergeNothing()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(
            mSchema,
            // Field schema
            "field1:int8;field2:uint8;field3:int16;field4:uint16;field5:int32;field6:uint32;field7:int64;field8:uint64",
            // Index schema
            "number1:number:field1;number2:number:field2;number3:number:field3;number4:number:field4;number5:number:"
            "field5;"
            "number6:number:field6;number7:number:field7;number8:number:field8"
            // Primary key index schema
            "",
            // Attribute schema
            "field1;field3;field5;field7",
            // Summary schema
            "field2;field4;field6;field8");

        string docStr = "{ 1, "
                        "[field1: ( -1 -10 )],"
                        "[field2: (0 10 250 255)],"
                        "[field3: (-1 -32768 0 5 32767)],"
                        "[field4: (0 20 65530 65535)],"
                        "[field5: (-1 -32769 0 0 15 65536)],"
                        "[field6: (0 20 65535)],"
                        "[field7: (-1 -1000000 20 20 65535)],"
                        "[field8: (0 20 65535 10000000)],"
                        "};"
                        "{ 3, "
                        "[field1: ( -10 )],"
                        "[field2: (0 110 250)],"
                        "[field3: (-32768 0 5 32767 9)],"
                        "[field4: (0 20 65534 888)],"
                        "[field5: (-32769 0  15 65536)],"
                        "[field6: (0 20 65534 123)],"
                        "[field7: (-1000000 20 20 65535)],"
                        "[field8: (0 20 65535 10000000)],"
                        "};";

        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        int64_t startTimestamp = 100;
        SetTimestamp(docVect, startTimestamp);

        // add document and dump.
        IndexBuilderPtr indexBuilder(
            new IndexBuilder(mRootDir, mOptions, mSchema, mQuotaControl, CommonBranchHinterOption::Test()));
        INDEXLIB_TEST_TRUE(indexBuilder->Init());

        // dump segment0
        indexBuilder->Build(docVect[0]);
        indexBuilder->EndIndex();
        indexBuilder->TEST_BranchFSMoveToMainRoad();

        Version version0;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version0, INVALID_VERSIONID);
        INDEXLIB_TEST_EQUAL((versionid_t)0, version0.GetVersionId());
        INDEXLIB_TEST_EQUAL(startTimestamp, version0.GetTimestamp());

        // merge nothing
        indexBuilder->Merge(mOptions);
        Version version1;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version1, INVALID_VERSIONID);
        INDEXLIB_TEST_EQUAL((versionid_t)1, version1.GetVersionId());
        INDEXLIB_TEST_EQUAL(startTimestamp, version1.GetTimestamp());
    }

    void InnerTestForAlignedVersionId(const string& dataDespStr, const string& mergeStrategyStr, bool noBuildVersion,
                                      int32_t alignedVersionId, size_t splitSegmentCount = 1)
    {
        tearDown();
        setUp();
        IndexPartitionOptions options;
        options.SetEnablePackageFile(false);
        options.GetMergeConfig().mergeStrategyStr = mergeStrategyStr;
        if (splitSegmentCount > 1) {
            options.GetMergeConfig().GetSplitSegmentConfig().className = TestSplitStrategy::STRATEGY_NAME;
            options.GetMergeConfig().GetSplitSegmentConfig().parameters["segment_count"] =
                std::to_string(splitSegmentCount);
            options.GetMergeConfig().GetSplitSegmentConfig().parameters["use_invalid_doc"] = "true";
        }
        SingleFieldPartitionDataProvider provider(options);
        string targetIndexPath = GET_TEMP_DATA_PATH();
        auto baseDir = GET_PARTITION_DIRECTORY();
        provider.Init(targetIndexPath, "int32", SFP_ATTRIBUTE);
        provider.Build(dataDespStr, SFP_OFFLINE);

        Version buildVersion;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(baseDir, buildVersion, INVALID_VERSIONID);
        if (buildVersion.GetVersionId() == 0 && noBuildVersion) {
            baseDir->RemoveFile("version.0");
        }

        uint32_t instanceCount = 1;
        bool optimizeMerge = true;
        IndexPartitionMergerPtr indexMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
                targetIndexPath, provider.GetOptions(), NULL, ""));

        MergeMetaPtr mergeMeta = indexMerger->CreateMergeMeta(optimizeMerge, instanceCount, 0);
        IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);

        indexMerger->DoMerge(optimizeMerge, mergeMeta, 0);

        if (alignedVersionId != INVALID_VERSIONID && alignedVersionId < mergeMeta->GetTargetVersion().GetVersionId()) {
            EXPECT_THROW(indexMerger->EndMerge(mergeMeta, alignedVersionId), IndexCollapsedException);
            return;
        }
        indexMerger->EndMerge(mergeMeta, alignedVersionId);
        Version mergeVersion;
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        VersionLoader::GetVersion(baseDir, mergeVersion, INVALID_VERSIONID);

        if (alignedVersionId != INVALID_VERSIONID) {
            ASSERT_EQ(alignedVersionId, mergeVersion.GetVersionId());
        } else { // alignedVersionId == INVALID_VERSIONID
            if (indexMergeMeta->GetMergePlans().size() == 0) {
                if (noBuildVersion) {
                    ASSERT_EQ(0, mergeVersion.GetVersionId());
                } else {
                    ASSERT_EQ(buildVersion.GetVersionId(), mergeVersion.GetVersionId());
                }
            } else {
                ASSERT_EQ(mergeMeta->GetTargetVersion().GetVersionId(), mergeVersion.GetVersionId());
            }
        }
    }

    void TestCaseForAlignedVersionId()
    {
        // test specify alignedVersionId
        InnerTestForAlignedVersionId("1,2,3", "optimize", false, 6);
        InnerTestForAlignedVersionId("1,2,3", "align_version", false, 6);
        // test specify alignedVersionId smaller than targetVersionId
        // InnerTestForAlignedVersionId("1,2,3", "optimize", false, 0);
        // InnerTestForAlignedVersionId("1,2,3", "align_version", false, 0);
        // test not specify alignedVersion
        InnerTestForAlignedVersionId("1,2,3", "optimize", false, INVALID_VERSIONID);
        // test empty mergeMeta with alignedVersionId
        InnerTestForAlignedVersionId("", "optimize", false, 6);
        // test empty mergeMeta with no alignedVersionId
        InnerTestForAlignedVersionId("", "optimize", false, INVALID_VERSIONID);
        // test empty mergeMeta with no buildVersion and with no alignedVersionId
        InnerTestForAlignedVersionId("", "optimize", true, INVALID_VERSIONID);
        // test empty mergeMeta with no buildVersion and with spcified alignedVersionId
        InnerTestForAlignedVersionId("", "optimize", true, 6);
    }

    void TestCaseForMergeMemControl()
    {
        string field = "string:string;text2:text;text:text;number:uint32;pk:uint64;"
                       "ends:uint32;nid:uint32";
        string index = "index_string:string:string;index_pk:primarykey64:pk;"
                       "index_text2:text:text2;index_text:text:text;index_number:number:number";
        string attribute = "string;number;pk;ends;nid";
        string summary = "string;number;pk";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        SchemaMaker::SetAdaptiveHighFrequenceDictionary("index_text2", "DOC_FREQUENCY#1", indexlib::index::hp_both,
                                                        mSchema);
        // for estimate mem use more than 80M
        TruncateParams param("3:10000000", "",
                             "index_text=ends:ends#DESC|nid#ASC:::;"
                             "index_text2=ends:ends#DESC|nid#ASC:::",
                             /*inputStrategyType=*/"default");
        TruncateConfigMaker::RewriteSchema(mSchema, param);
        mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);
        // use mem less than need, merge change maxMemUse
        mOptions.GetMergeConfig().maxMemUseForMerge = 1;
        string fullDocs = "cmd=add,string=hello1,text=text1 text2 text3,"
                          "text2=text1 text2 text3,number=1,pk=1,ends=1,nid=1;"
                          "cmd=add,string=hello2,text=text1 text2 text3,"
                          "text2=text1 text2 text3,number=2,pk=2,ends=2,nid=2;";

        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));

        {
            SetTargetSegmentCount(2);
            PartitionStateMachine psm;
            ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
            ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
        }
    }

    void TestAlignVersion()
    {
        string field = "string:string;text2:text;text:text;number:uint32;pk:uint64;"
                       "ends:uint32;nid:uint32";
        string index = "index_string:string:string;index_pk:primarykey64:pk;"
                       "index_text2:text:text2;index_text:text:text;index_number:number:number";
        string attribute = "string;number;pk;ends;nid";
        string summary = "string;number;pk";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        string fullDocs = "cmd=add,string=hello1,text=text1 text2 text3,"
                          "text2=text1 text2 text3,number=1,pk=1,ends=1,nid=1;"
                          "cmd=add,string=hello2,text=text1 text2 text3,"
                          "text2=text1 text2 text3,number=2,pk=2,ends=2,nid=2;";

        PartitionStateMachine psm;
        string indexRoot = GET_TEMP_DATA_PATH();
        ASSERT_TRUE(psm.Init(mSchema, mOptions, indexRoot));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index_number:1", "nid=1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index_number:2", "nid=2"));

        string incDocString = "cmd=add,string=hello1,text=text1 text2 text3,"
                              "text2=text1 text2 text3,number=3,pk=1,ends=1,nid=3;"
                              "cmd=add,string=hello2,text=text1 text2 text3,"
                              "text2=text1 text2 text3,number=4,pk=2,ends=2,nid=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index_number:3", "nid=3"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index_number:4", "nid=4"));

        {
            MergeConfig& config = mOptions.GetMergeConfig();
            config.mergeStrategyStr = "align_version";
            IndexPartitionMergerPtr merger(
                (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(indexRoot, mOptions,
                                                                                                NULL, indexRoot));
            versionid_t targetVersion = 3;
            merger->PrepareMerge(1234);
            MergeMetaPtr mergeMeta = merger->CreateMergeMeta(true, 1, 1234);
            merger->DoMerge(true, mergeMeta, 0);
            merger->EndMerge(mergeMeta, targetVersion);
        }
        // check version3 is generated
        Version version2, version3;
        VersionLoader::GetVersionS(GET_TEMP_DATA_PATH(), version2, 2);
        VersionLoader::GetVersionS(GET_TEMP_DATA_PATH(), version3, 3);
    }

    void TestEndMergeWithVersion()
    {
        string field = "string:string;text2:text;text:text;number:uint32;pk:uint64;"
                       "ends:uint32;nid:uint32";
        string index = "index_string:string:string;index_pk:primarykey64:pk;"
                       "index_text2:text:text2;index_text:text:text;index_number:number:number";
        string attribute = "string;number;pk;ends;nid";
        string summary = "string;number;pk";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        string fullDocs = "cmd=add,string=hello1,text=text1 text2 text3,"
                          "text2=text1 text2 text3,number=1,pk=1,ends=1,nid=1;"
                          "cmd=add,string=hello2,text=text1 text2 text3,"
                          "text2=text1 text2 text3,number=2,pk=2,ends=2,nid=2;";

        PartitionStateMachine psm;
        string indexRoot = GET_TEMP_DATA_PATH();
        ASSERT_TRUE(psm.Init(mSchema, mOptions, indexRoot));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));

        IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
            indexRoot, mOptions, NULL, indexRoot));
        int64_t ts = 123;
        versionid_t targetVersion = 3;
        merger->PrepareMerge(ts);
        MergeMetaPtr mergeMeta = merger->CreateMergeMeta(true, 1, ts);
        merger->DoMerge(true, mergeMeta, 0);
        merger->EndMerge(mergeMeta, targetVersion);

        string deployMeta = indexRoot + "/" + DeployIndexWrapper::GetDeployMetaFileName(targetVersion);
        fslib::FileMeta meta;
        ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(deployMeta, meta));
        // merge again with version is genearate, will not redo endMerge
        merger->EndMerge(mergeMeta, targetVersion);

        fslib::FileMeta meta2;
        ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(deployMeta, meta2));
        ASSERT_EQ(meta.createTime, meta2.createTime);
        // mock segment is broken, end merge will not generate new deployMeta & version
        {
            string segInfo0 = indexRoot + "/segment_1_level_0/segment_info";
            ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(segInfo0, segInfo0 + ".bak").Code());
            FslibWrapper::DeleteFileE(segInfo0, DeleteOption::NoFence(false));
            EXPECT_THROW(PartitionMergerCreator::TEST_CreateSinglePartitionMerger(indexRoot, mOptions, NULL, indexRoot),
                         IndexCollapsedException);
            ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(segInfo0 + ".bak", segInfo0).Code());
        }
        // // mock target version is broken, end merge will generate new deployMeta & version
        // fslib::FileMeta meta4;
        // {
        //     string versionFile = indexRoot + "/" + Version::GetVersionFileName(targetVersion);
        //     string content;
        //     FslibWrapper::AtomicLoadE(versionFile, content);
        //     FslibWrapper::AtomicStoreE(versionFile, content.substr(0, content.size()-10));
        //     sleep(2);
        //     merger->EndMerge(mergeMeta, targetVersion);
        //     ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(deployMeta, meta4));
        //     ASSERT_GT(meta4.createTime, meta2.createTime);
        //     ASSERT_TRUE(FslibWrapper::IsExist(versionFile).GetOrThrow());
        // }
        // mock target version is not generate, end merge will generate new deployMeta & version
        fslib::FileMeta meta5;
        {
            int64_t curTs = autil::TimeUtility::currentTimeInSeconds();
            sleep(2);
            string versionFile = indexRoot + "/" + Version::GetVersionFileName(targetVersion);
            FslibWrapper::DeleteFileE(versionFile, DeleteOption::NoFence(false));
            merger.reset((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
                indexRoot, mOptions, NULL, indexRoot));
            merger->EndMerge(mergeMeta, targetVersion);
            ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(deployMeta, meta5));
            ASSERT_GT(meta5.createTime, meta2.createTime);
            ASSERT_TRUE(FslibWrapper::IsExist(versionFile).GetOrThrow());
            // check deploy_index segment_file_list is regenerated
            string deployIndex = indexRoot + "/segment_1_level_0/" + DEPLOY_INDEX_FILE_NAME;
            string segFileList = indexRoot + "/segment_1_level_0/" + SEGMENT_FILE_LIST;
            fslib::FileMeta deployIndexMeta;
            if (FslibWrapper::IsExist(deployIndex).GetOrThrow()) {
                ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(deployIndex, deployIndexMeta));
                ASSERT_GT((int64_t)deployIndexMeta.createTime, curTs);
            }

            if (FslibWrapper::IsExist(segFileList).GetOrThrow()) {
                ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(segFileList, deployIndexMeta));
                ASSERT_GT((int64_t)deployIndexMeta.createTime, curTs);
            }
        }
    }

    void TestCaseForTTL()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                                         // Field schema
                                         "field1:int8",
                                         // Primary key index schema
                                         "pk:primarykey64:field1",
                                         // Attribute schema
                                         "field1",
                                         // Summary schema
                                         "field1");
        mSchema->SetEnableTTL(true);

        string docStrNotMerge = "{ 4, "
                                "[field1: ( 4 )],"
                                "[doc_time_to_live_in_seconds: (1)],"
                                "};"
                                "{ 5, "
                                "[field1: ( 5 )],"
                                "[doc_time_to_live_in_seconds: (2)],"
                                "};"
                                "{ 6, "
                                "[field1: ( 6 )],"
                                "[doc_time_to_live_in_seconds: (3)],"
                                "};"
                                "{ 7, "
                                "[field1: ( 7 )],"
                                "[doc_time_to_live_in_seconds: (1)],"
                                "};"
                                "{ 8, "
                                "[field1: ( 8 )],"
                                "[doc_time_to_live_in_seconds: (2)],"
                                "};"
                                "{ 9, "
                                "[field1: ( 9 )],"
                                "[doc_time_to_live_in_seconds: (4000000000)],"
                                "};";

        string docStr = "{ 1, "
                        "[field1: ( -1 -10 )],"
                        "[doc_time_to_live_in_seconds: (65535)],"
                        "};"
                        "{ 3, "
                        "[field1: ( -10 )],"
                        "[doc_time_to_live_in_seconds: (4294967293)],"
                        "};"
                        "{ 2, "
                        "[field1: ( -120 )],"
                        "[doc_time_to_live_in_seconds: (4294967295)],"
                        "};";

        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("base-doc-count=1;max-doc-count=2;"
                                                                         "conflict-segment-number=3;");

        mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 5;

        DocumentMaker::DocumentVect docVect;
        DocumentMaker::DocumentVect docVectNotMerge;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        DocumentMaker::MakeDocuments(docStrNotMerge, mSchema, docVectNotMerge, mockIndexPart);

        IndexBuilderPtr builder(
            new IndexBuilder(mRootDir, mOptions, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test()));
        INDEXLIB_TEST_TRUE(builder->Init());
        for (size_t i = 0; i < 3; i++) {
            builder->Build(docVectNotMerge[i]);
        }
        builder->DumpSegment();
        for (size_t i = 3; i < 6; i++) {
            builder->Build(docVectNotMerge[i]);
        }
        builder->DumpSegment();
        for (size_t i = 0; i < docVect.size(); i++) {
            builder->Build(docVect[i]);
            builder->DumpSegment();
        }
        builder->EndIndex();
        builder->TEST_BranchFSMoveToMainRoad();

        OnlinePartition indexPart;
        indexPart.Open(mRootDir, "", mSchema, mOptions);
        IndexPartitionReaderPtr reader = indexPart.GetReader();
        const index::PartitionMetrics& partMetrics = reader->GetPartitionInfo()->GetPartitionMetrics();
        INDEXLIB_TEST_EQUAL((docid_t)9, partMetrics.totalDocCount);
        INDEXLIB_TEST_EQUAL((docid_t)0, partMetrics.delDocCount);

        builder->Merge(mOptions, false);
        indexPart.ReOpen(false);
        reader = indexPart.GetReader();
        AttributeReaderPtr attrReader = reader->GetAttributeReader(DOC_TIME_TO_LIVE_IN_SECONDS);
        string ts;
        attrReader->Read(0, ts);
        INDEXLIB_TEST_EQUAL("1", ts);
        attrReader->Read(1, ts);
        INDEXLIB_TEST_EQUAL("2", ts);
        attrReader->Read(2, ts);
        INDEXLIB_TEST_EQUAL("4000000000", ts);
        attrReader->Read(3, ts);
        INDEXLIB_TEST_EQUAL("4294967293", ts);
        attrReader->Read(4, ts);
        INDEXLIB_TEST_EQUAL("4294967295", ts);
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        checkTTL("segment_0_level_0", 3);
        checkTTL("segment_1_level_0", 4000000000);
        checkTTL("segment_2_level_0", 65535);
        checkTTL("segment_3_level_0", 4294967293);
        checkTTL("segment_4_level_0", 4294967295);
        checkTTL("segment_6_level_0", 4294967295);
    }

private:
    void checkTTL(const string& segment, uint32_t ttl)
    {
        ASSERT_TRUE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH(segment)).GetOrThrow());
        SegmentInfo segInfo;
        segInfo.TEST_Load(GET_TEMP_DATA_PATH(segment + "/" + SEGMENT_INFO_FILE_NAME));
        INDEXLIB_TEST_EQUAL(ttl, segInfo.maxTTL);
    }
    void MakeIndex(const SegVect& segments, segmentid_t deleteOnlySegId, MockIndexPart& mockIndexPart)
    {
        docid_t baseDocId = 0;
        for (size_t i = 0; i < segments.size(); ++i) {
            if (deleteOnlySegId != INVALID_SEGMENTID && (segmentid_t)i == deleteOnlySegId) {
                MakeIndexDataWithDelSegment(segments[i].first, mockIndexPart, baseDocId, segments[i].second);
            } else {
                MakeIndexData(segments[i].first, mockIndexPart, baseDocId, segments[i].second);
            }

            baseDocId += segments[i].first;
        }
    }

    void ClearRootDir()
    {
        if (FslibWrapper::IsExist(mRootDir).GetOrThrow()) {
            FslibWrapper::DeleteDirE(mRootDir, DeleteOption::NoFence(false));
        }

        FslibWrapper::MkDirE(mRootDir);
        RESET_FILE_SYSTEM("ut", false);
    }

    void MergeIndex(const string& mergeTaskStr, MergeTask& mergeTask, bool needStore = true, bool optimize = false)
    {
        RESET_FILE_SYSTEM("ut", false, file_system::FileSystemOptions::OfflineWithBlockCache(nullptr));
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        Version version;
        VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
        mSegmentDir.reset(new SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
        mSegmentDir->Init(mSchema->GetSubIndexPartitionSchema() != NULL, true);

        IndexPartitionOptions option = mOptions;
        option.SetIsOnline(false);

        if (mAsync) {
            option.GetMergeConfig().mergeIOConfig.enableAsyncRead = mAsync;
            option.GetMergeConfig().mergeIOConfig.enableAsyncWrite = mAsync;
        }

        IndexPartitionSchemaPtr rewriteSchema(mSchema->Clone());
        SchemaAdapter::RewritePartitionSchema(rewriteSchema, GET_PARTITION_DIRECTORY(), option);

        if (!mergeTaskStr.empty()) {
            auto segMergeInfos =
                MergeMetaCreator::CreateSegmentMergeInfos(rewriteSchema, option.GetMergeConfig(), mSegmentDir);
            MergeTaskMaker::CreateMergeTask(mergeTaskStr, mergeTask, segMergeInfos);

            MockIndexPartitionMerger merger(mSegmentDir, rewriteSchema, option);
            EXPECT_CALL(merger, CreateMergeTaskByStrategy(_, _, _, _)).WillRepeatedly(Return(mergeTask));
            merger.Merge();
        } else {
            if (!needStore) {
                {
                    IndexPartitionMergerToGetMergeTask merger(mSegmentDir, rewriteSchema, option);
                    MergeMetaPtr mergeMeta = merger.CreateMergeMeta(false, 1, 0);
                    IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);

                    const auto& mergePlans = indexMergeMeta->GetMergePlans();
                    if (!mergePlans.empty()) {
                        segmentid_t targetSegmentId = mergePlans[0].GetTargetSegmentId(0);
                        string segment = indexMergeMeta->GetTargetVersion().GetSegmentDirName(targetSegmentId);
                        file_system::DirectoryPtr segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory(segment);
                        segmentDir->Store("segment_info", "");
                        segmentDir->MakeDirectory("attribute/long1")->Store("data", "");
                        segmentDir->MakeDirectory("sub_segment/attribute/sub_long1")->Store("data", "");
                    }
                }
                IndexPartitionMergerToGetMergeTask merger(mSegmentDir, rewriteSchema, option);
                merger.Merge(false);
                mergeTask = merger.GetMergeTask();
            } else {
                string mergeMetaRoot = GET_TEMP_DATA_PATH("merge_meta");
                {
                    IndexPartitionMergerToGetMergeTask merger(mSegmentDir, rewriteSchema, option);
                    MergeMetaPtr mergeMeta = merger.CreateMergeMeta(false, 1, 0);
                    mergeMeta->Store(mergeMetaRoot);
                    mergeTask = merger.GetMergeTask();
                }
                {
                    MergeMetaPtr mergeMeta(new IndexMergeMeta());
                    mergeMeta->Load(mergeMetaRoot);
                    IndexPartitionMerger merger(mSegmentDir, rewriteSchema, option, merger::DumpStrategyPtr(), NULL,
                                                plugin::PluginManagerPtr(), CommonBranchHinterOption::Test());
                    merger.DoMerge(optimize, mergeMeta, 0);
                }
                {
                    MergeMetaPtr mergeMeta(new IndexMergeMeta());
                    mergeMeta->LoadBasicInfo(mergeMetaRoot);
                    IndexPartitionMerger merger(mSegmentDir, rewriteSchema, option, merger::DumpStrategyPtr(), NULL,
                                                plugin::PluginManagerPtr(), CommonBranchHinterOption::Test());
                    merger.EndMerge(mergeMeta, INVALID_VERSIONID);
                }
            }
        }
    }

    void DoInternalTestMerge(const SegVect& segments, const string& mergeTaskStr, segmentid_t deleteOnlySegId,
                             bool needStore, size_t splitSegmentCount)
    {
        SetTargetSegmentCount(splitSegmentCount);
        MockIndexPart mockIndexPart;

        MakeIndex(segments, deleteOnlySegId, mockIndexPart);

        MergeTask mergeTask;
        MergeIndex(mergeTaskStr, mergeTask, needStore);

        MockIndexPart newMockIndexPart;
        ReclaimMockIndexWithTask(mockIndexPart, segments, mergeTask, newMockIndexPart);
        CheckIndex(newMockIndexPart, false);
    }

    void InternalTestMerge(const SegVect& segments, const string& mergeTaskStr = "",
                           segmentid_t deleteOnlySegId = INVALID_SEGMENTID, vector<size_t> splitSegmentCounts = {1, 2},
                           bool needEmptyVersion = false)
    {
        SegVect emptySegments;
        emptySegments.push_back(make_pair(1, IndexTestUtil::DM_DELETE_ALL));
        for (size_t i = 0, size = splitSegmentCounts.size(); i < size; ++i) {
            if (i > 0) {
                ClearRootDir();
            }
            if (needEmptyVersion) {
                DoInternalTestMerge(emptySegments, "", INVALID_SEGMENTID, true, 1);
            }
            size_t splitCount = splitSegmentCounts[i];
            DoInternalTestMerge(segments, mergeTaskStr, deleteOnlySegId, true, splitCount);
            ClearRootDir(); // to clear last merge info
            if (needEmptyVersion) {
                DoInternalTestMerge(emptySegments, "", INVALID_SEGMENTID, true, 1);
            }
            DoInternalTestMerge(segments, mergeTaskStr, deleteOnlySegId, false, splitCount);
        }
    }

    void MakeIndexWithSub(const SegVect& segments, const SegVect& subSegments, segmentid_t deleteOnlySegId,
                          MockIndexPart& mockIndexPart)

    {
        docid_t baseDocId = 0;
        for (size_t i = 0; i < segments.size(); ++i) {
            uint32_t docCount = segments[i].first;
            DeletionMode delMode = segments[i].second;
            OfflinePartition indexPart4Write;
            IndexPartitionOptions options = mOptions;
            options.SetIsOnline(false);
            indexPart4Write.Open(mRootDir, "", mSchema, options);

            IndexPartitionMaker::MakeOneSegmentData(docCount, indexPart4Write, mockIndexPart, baseDocId, delMode);

            baseDocId += docCount;
        }
    }

    void InternalTestMergeWithSub(const SegVect& segments, const SegVect& subSegments, const string& mergeTaskStr,
                                  segmentid_t deleteOnlySegId, bool needStore)
    {
        MockIndexPart mockIndexPart;
        MockIndexPart subMockIndexPart;
        mockIndexPart.subIndexPart = &subMockIndexPart;
        // TODO: support subSegments or ratio
        MakeIndexWithSub(segments, segments, deleteOnlySegId, mockIndexPart);

        MergeTask mergeTask;
        MergeIndex(mergeTaskStr, mergeTask, needStore);

        MockIndexPart newMockIndexPart;
        ReclaimMockIndexWithTask(mockIndexPart, segments, mergeTask, newMockIndexPart);
        CheckIndex(newMockIndexPart, false);

        MockIndexPart newSubMockIndexPart;
        ReclaimMockIndexWithTask(subMockIndexPart, segments, mergeTask, newSubMockIndexPart);
        CheckIndex(newSubMockIndexPart, true);
    }

    void InternalTestMergeWithSub(const SegVect& segments, const SegVect& subSegments, const string& mergeTaskStr = "",
                                  segmentid_t deleteOnlySegId = INVALID_SEGMENTID)
    {
        InternalTestMergeWithSub(segments, subSegments, mergeTaskStr, deleteOnlySegId, /*needStore=*/true);
        ClearRootDir(); // to clear last merge info
        InternalTestMergeWithSub(segments, subSegments, mergeTaskStr, deleteOnlySegId, /*needStore=*/false);
    }

    ReclaimMapPtr CreateReclaimMap(const MockIndexPart& mockIndexPart)
    {
        const MockDeletionMap& deletionMap = mockIndexPart.deletionMap;
        uint32_t docCount = mockIndexPart.docCount;

        ReclaimMapPtr reclaimMap(new ReclaimMap);
        reclaimMap->Init(docCount);

        docid_t newDocId = 0;
        MockDeletionMap::const_iterator it = deletionMap.begin();
        for (docid_t docId = 0; docId < (docid_t)docCount; ++docId) {
            if (docId == *it) {
                reclaimMap->SetNewId(docId, INVALID_DOCID);
                ++it;
            } else {
                reclaimMap->SetNewId(docId, newDocId);
                ++newDocId;
            }
        }
        assert(it == deletionMap.end());
        return reclaimMap;
    }

    void CalcMergedSegInfos(const SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
                            const MockDeletionMap& deletionMap, SegmentMergeInfos& mergedSegInfos,
                            uint32_t& totalDocCount)
    {
        totalDocCount = 0;
        docid_t newBaseDocId = 0;
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];

            if (!plan.HasSegment(segMergeInfo.segmentId)) {
                SegmentMergeInfo newSegMergeInfo = segMergeInfo;
                newSegMergeInfo.baseDocId = newBaseDocId;

                mergedSegInfos.push_back(newSegMergeInfo);

                newBaseDocId += segMergeInfo.segmentInfo.docCount;
            }
            totalDocCount += segMergeInfo.segmentInfo.docCount;
        }

        uint32_t newSegBaseId = newBaseDocId;
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
            if (!plan.HasSegment(segMergeInfo.segmentId)) {
                continue;
            }

            uint32_t delDocCount = 0;
            for (docid_t j = 0; j < (docid_t)segMergeInfo.segmentInfo.docCount; ++j) {
                MockDeletionMap::const_iterator it = deletionMap.find(j + segMergeInfo.baseDocId);
                if (it != deletionMap.end()) {
                    ++delDocCount;
                }
            }
            newBaseDocId += (segMergeInfo.segmentInfo.docCount - delDocCount);
        }

        size_t segmentCount = GetTargetSegmentCount();
        size_t totalDoc = newBaseDocId - newSegBaseId;
        vector<docid_t> splitDocCountVec(segmentCount, totalDoc / segmentCount);
        docid_t leftSize = totalDoc % segmentCount;
        for (int i = 0; i < leftSize; ++i) {
            splitDocCountVec[i]++;
        }
        vector<docid_t> baseDocIdVec(segmentCount, 0);
        for (size_t i = 1; i < segmentCount; ++i) {
            baseDocIdVec[i] = baseDocIdVec[i - 1] + splitDocCountVec[i - 1];
        }
        for (size_t i = 0; i < segmentCount; ++i) {
            SegmentMergeInfo newSegMergeInfo;
            newSegMergeInfo.baseDocId = newSegBaseId + baseDocIdVec[i];
            newSegMergeInfo.deletedDocCount = 0;
            newSegMergeInfo.segmentInfo.docCount = splitDocCountVec[i];
            newSegMergeInfo.segmentId = segMergeInfos[segMergeInfos.size() - 1].segmentId + i + 1;
            mergedSegInfos.push_back(newSegMergeInfo);
        }
    }

    void ReclaimMockIndexWithTask(const MockIndexPart& mockIndexPart, const SegVect& segments,
                                  const MergeTask& mergeTask, MockIndexPart& newMockIndexPart)
    {
        SegmentMergeInfos segMergeInfos;
        segMergeInfos.resize(segments.size());
        uint32_t baseDocId = 0;
        for (size_t i = 0; i < segments.size(); ++i) {
            SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
            segMergeInfo.segmentId = (segmentid_t)i;
            segMergeInfo.baseDocId = baseDocId;
            segMergeInfo.segmentInfo.docCount = segments[i].first;
            baseDocId += segments[i].first;
        }

        newMockIndexPart = mockIndexPart;
        for (size_t i = 0; i < mergeTask.GetPlanCount(); ++i) {
            SegmentMergeInfos mergedSegInfos;
            ReclaimMapPtr reclaimMap =
                CreateReclaimMapWithPlan(newMockIndexPart.deletionMap, segMergeInfos, mergeTask[i], mergedSegInfos);

            MockIndexPart tmpMockIndexPart;
            DocumentMaker::ReclaimDocuments(newMockIndexPart, reclaimMap, tmpMockIndexPart);

            segMergeInfos = mergedSegInfos;
            newMockIndexPart = tmpMockIndexPart;
        }
    }

    ReclaimMapPtr CreateReclaimMapWithPlan(const MockDeletionMap& deletionMap, const SegmentMergeInfos& segMergeInfos,
                                           const MergePlan& mergePlan, SegmentMergeInfos& mergedSegInfos)
    {
        uint32_t totalDocCount = 0;
        CalcMergedSegInfos(segMergeInfos, mergePlan, deletionMap, mergedSegInfos, totalDocCount);

        ReclaimMapPtr reclaimMap(new ReclaimMap);
        reclaimMap->Init(totalDocCount);
        size_t segmentCount = GetTargetSegmentCount();
        size_t notInPlanCount = 0;
        vector<docid_t> baseDocIdVec(mergedSegInfos.size());
        // for test_split_strategy.h
        for (size_t i = 0, size = baseDocIdVec.size(); i < size; ++i) {
            baseDocIdVec[i] = mergedSegInfos[i].baseDocId;
        }
        size_t outputDocBaseIndex = mergedSegInfos.size() - segmentCount;
        size_t invalidDocCount = 0;
        (void)invalidDocCount;
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            const SegmentMergeInfo& segInfo = segMergeInfos[i];
            if (mergePlan.HasSegment(segInfo.segmentId)) {
                for (docid_t j = 0; j < (docid_t)segInfo.segmentInfo.docCount; ++j) {
                    docid_t oldDocId = j + segInfo.baseDocId;
                    if (deletionMap.find(oldDocId) != deletionMap.end()) {
                        reclaimMap->SetNewId(oldDocId, INVALID_DOCID);
                    } else {
                        docid_t newId = baseDocIdVec[outputDocBaseIndex + (invalidDocCount++ % segmentCount)]++;
                        reclaimMap->SetNewId(oldDocId, newId);
                    }
                }
            } else {
                for (docid_t j = 0; j < (docid_t)segInfo.segmentInfo.docCount; ++j) {
                    docid_t oldDocId = j + segInfo.baseDocId;
                    reclaimMap->SetNewId(oldDocId, baseDocIdVec[notInPlanCount]++);
                }
                ++notInPlanCount;
            }
        }

        return reclaimMap;
    }

    void CheckIndex(const MockIndexPart& mockIndexPart, bool isSub)
    {
        string root = mRootDir;
        IndexPartitionSchemaPtr schema = mSchema;
        OnlinePartition indexPart4Read;
        indexPart4Read.Open(root, "", schema, mOptions);

        IndexPartitionReaderPtr reader = indexPart4Read.GetReader();
        if (isSub) {
            schema = mSchema->GetSubIndexPartitionSchema();
            reader = reader->GetSubPartitionReader();
        }

        DocumentCheckerForGtest checker;
        checker.CheckData(reader, schema, mockIndexPart);
    }

    void MakeIndexData(uint32_t docCount, MockIndexPart& mockIndexPart, int32_t pkStartSuffix, DeletionMode delMode)
    {
        string root = mRootDir;
        IndexPartitionSchemaPtr schema = mSchema;
        OfflinePartition indexPart4Write;
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        indexPart4Write.Open(root, "", schema, options);
        IndexPartitionMaker::MakeOneSegmentData(docCount, indexPart4Write, mockIndexPart, pkStartSuffix, delMode);
    }

    void MakeEmptyDocsAndDump()
    {
        string docStr = "{};{};{}";
        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);

        IndexBuilder builder(mRootDir, mOptions, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(builder.Init());

        for (size_t i = 0; i < docVect.size(); ++i) {
            builder.Build(docVect[i]);
        }
        builder.EndIndex();
        builder.TEST_BranchFSMoveToMainRoad();
    }
    void MakeIndexDataWithDelSegment(uint32_t docCount, MockIndexPart& mockIndexPart, docid_t baseDocId,
                                     DeletionMode delMode)
    {
        if (delMode == IndexTestUtil::DM_NODELETE) {
            MakeIndexData(docCount, mockIndexPart, baseDocId, delMode);
            return;
        }
        string root = mRootDir;
        IndexPartitionSchemaPtr schema = mSchema;
        MockIndexPart tmpIndexPart;
        MakeIndexData(docCount, tmpIndexPart, baseDocId, IndexTestUtil::DM_NODELETE);

        OfflinePartition indexPart4Write;
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        indexPart4Write.Open(root, "", schema, options);
        IndexPartitionMaker::MakeOneDelSegmentData(docCount, indexPart4Write, mockIndexPart, baseDocId, delMode);
    }

    void CreateOneSegmentData(uint32_t docCount, const IndexPartitionOptions& options, int64_t timestamp)
    {
        IndexBuilder builder(mRootDir, options, mSchema, mQuotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(builder.Init());

        uint32_t pkSuffix = 0;
        string docStr;
        IndexPartitionSchemaPtr schema = builder.GetIndexPartition()->GetSchema();
        DocumentMaker::MakeDocumentsStr(docCount, schema->GetFieldSchema(), docStr, "string1", pkSuffix);

        DocumentMaker::DocumentVect segDocVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, schema, segDocVect, mockIndexPart);

        for (size_t i = 0; i < segDocVect.size(); ++i) {
            segDocVect[i]->SetTimestamp(timestamp);
            builder.Build(segDocVect[i]);
        }
        builder.EndIndex();
        builder.TEST_BranchFSMoveToMainRoad();
    }

    void SetTimestamp(DocumentMaker::DocumentVect& docVec, int64_t startTimestamp)
    {
        for (size_t i = 0; i < docVec.size(); i++) {
            docVec[i]->SetTimestamp(startTimestamp++);
        }
    }

    void MakeIndexPartition(const string rootDir, const IndexPartitionSchemaPtr schema,
                            const IndexPartitionOptions& options, const DocumentMaker::DocumentVect docVect,
                            const bool needMerge)
    {
        // add document and dump.
        IndexBuilder builder(rootDir, options, schema, mQuotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(builder.Init());

        for (size_t i = 0; i < docVect.size(); i++) {
            builder.Build(docVect[i]);
        }
        builder.EndIndex();
        builder.TEST_BranchFSMoveToMainRoad();
        if (needMerge) {
            builder.Merge(options);
        }
    }

    void SetTargetSegmentCount(size_t splitSegmentCount)
    {
        if (splitSegmentCount > 1) {
            mOptions.GetMergeConfig().GetSplitSegmentConfig().className = TestSplitStrategy::STRATEGY_NAME;
            mOptions.GetMergeConfig().GetSplitSegmentConfig().parameters["segment_count"] =
                std::to_string(splitSegmentCount);
            mOptions.GetMergeConfig().GetSplitSegmentConfig().parameters["use_invalid_doc"] = "true";
        }
    }

    size_t GetTargetSegmentCount()
    {
        string segmentCountStr =
            GetValueFromKeyValueMap(mOptions.GetMergeConfig().GetSplitSegmentConfig().parameters, "segment_count", "1");
        size_t segmentCount;
        if (!(autil::StringUtil::fromString(segmentCountStr, segmentCount)) || segmentCount < 1) {
            return 1;
        }
        return segmentCount;
    }

public:
    string mRootDir;
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionSchemaPtr mSubSchema;
    IndexPartitionOptions mOptions;
    SegmentDirectoryPtr mSegmentDir;
    bool mAsync;
    QuotaControlPtr mQuotaControl;
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestMergeEmptyDocuments);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimize);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeWithAsync);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeWithDeleteEven);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeWithDeleteSome);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeWithDeleteMany);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeWithLastDeleteAll);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeWithFirstDeleteAll);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeWithDeleteAll);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeOneSegment);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeOneSegmentWithDeleteEven);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeOneSegmentWithAllDelete);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeManySegmentManyDocsLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForOptimizeManySegManyDocsWithDelLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeWithPlanAndNoDelete);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeWithTwoPlanAndNoDelete);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeWithThreePlanAndSomeDeleteLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeWithTwoPlanAndOneMergeAllDelete);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeLeavingSomeDeletion);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForBalanceTreeStrategy);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForBalanceTreeStrategyWithDelPercent);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForBalanceTreeStrategyTestLimit);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForBalanceTreeStrategyMerge3Step);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForPriorityQueueStrategy);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForPriorityQueueStrategyWithDelPercent);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest,
                        TestCaseForPriorityQueueStrategyTestLimitMaxMergedSegmentDocCount);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForPriorityQueueStrategyTestLimitMaxTotalMergeDocCount);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForPriorityQueueStrategyWithMaxSegmentSize);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestOneSegmentWithNoDel);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForTimestampInSegmentInfo);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForTruncateAndBalanceTreeStrategy);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForDeployIndexWithTruncate);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForNumberIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForPackIndexWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForVersionAfterMerge);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForVersionAfterMergeNothing);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForAlignedVersionId);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForTTL);

INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForSubDirWithDeleteAll);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForSubDirWithDeleteEven);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForSubDirWithDeleteEvenSplitMode);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForSubDirWithDeleteSome);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeSubWithThreePlanAndSomeDeleteLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForSubDirWithNoDelete);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeFailOverCleanTargetDirs);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestCaseForMergeMemControl);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestEndMergeWithVersion);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerInteTest, TestAlignVersion);
}} // namespace indexlib::merger
