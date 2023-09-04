#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/merger_util/truncate/bucket_map_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::util;
namespace indexlib::index::legacy {

class TruncateIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    typedef DocumentMaker::MockIndexPart MockIndexPart;

public:
    DECLARE_CLASS_NAME(TruncateIndexWriterTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEMP_DATA_PATH();
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                                         "price:float;user_id:long;text1:text", // above is field schema
                                         "text:text:text1;",                    // above is index schema
                                         "price;user_id",                       // above is attribute schema
                                         "price");                              // above is summary schema

        string docStr = "{ 1, "
                        "[text1: (token1^3 token2) (token2^2)],"
                        "[price: (1.5)],"
                        "[user_id: (100)],"
                        "};";
        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        util::PartitionMemoryQuotaControllerPtr memoryController =
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        util::MetricProviderPtr metricProvider;
        util::CounterMapPtr counterMap;
        plugin::PluginManagerPtr pluginManager;
        BuildingPartitionParam param(options, mSchema, memoryController, container, counterMap, pluginManager,
                                     metricProvider, document::SrcSignature());
        PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
        partition::OfflinePartitionWriter writer(options, container);
        writer.Open(mSchema, partitionData, PartitionModifierPtr());
        writer.AddDocument(docVect[0]);
        writer.Close();
    }

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        ArchiveFolderPtr archiveFolder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, archiveFolder->Open(
                               GET_PARTITION_DIRECTORY()->MakeDirectory(TRUNCATE_META_DIR_NAME)->GetIDirectory()));
        TruncateIndexWriterPtr indexWriter = CreateIndexWriter(archiveFolder);
        INDEXLIB_TEST_TRUE(indexWriter.get() != NULL);
        IndexPartitionOptions options;
        partition::OnlinePartitionReader partitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr());
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        auto memoryController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        util::MetricProviderPtr metricProvider;
        util::CounterMapPtr counterMap;
        plugin::PluginManagerPtr pluginManager;
        BuildingPartitionParam param(options, mSchema, memoryController, container, counterMap, pluginManager,
                                     metricProvider, document::SrcSignature());
        PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
        partitionReader.Open(partitionData);
        std::shared_ptr<PostingIterator> postIter = CreatePosting("token2", partitionReader);
        indexWriter->AddPosting(index::DictKeyInfo(100), postIter);
        indexWriter->EndPosting();
        ASSERT_EQ(FSEC_OK, archiveFolder->Close());
    }

private:
    TruncateIndexWriterPtr CreateIndexWriter(const ArchiveFolderPtr& archiveFolder)
    {
        TruncateParams param("1:1", "::", "text=text_price:price#ASC:::", /*inputStrategyType=*/"default");
        IndexPartitionOptions options;
        options.GetMergeConfig().truncateOptionConfig =
            TruncateConfigMaker::MakeConfig(param.truncCommonStr, param.distinctStr, param.truncIndexConfigStr);
        SegmentMergeInfos mergeInfos;
        segmentid_t segId = 0;
        SegmentInfo segInfo;
        segInfo.docCount = 1;
        segInfo.SetLocator(indexlibv2::framework::Locator(0, segId));
        SegmentMergeInfo segMergeInfo(segId, segInfo, 0, 0);
        mergeInfos.push_back(segMergeInfo);

        vector<uint32_t> docCounts;
        docCounts.push_back(1);
        vector<set<docid_t>> delDocIdSets(1);
        ReclaimMapPtr reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets, true);
        string mergedDir = mRootDir;

        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(GET_PARTITION_DIRECTORY(), 1);
        segDir->Init(false);
        index_base::OutputSegmentMergeInfo outputSegMergeInfo;
        index_base::OutputSegmentMergeInfos outputSegMergeInfos;
        outputSegMergeInfo.path = PathUtil::JoinPath(mRootDir, "index");
        outputSegMergeInfo.directory = test::DirectoryCreator::Create(outputSegMergeInfo.path);
        outputSegMergeInfos.push_back(outputSegMergeInfo);
        TruncateIndexWriterCreator creator;
        TruncateAttributeReaderCreator attrReaderCreator(segDir, mSchema->GetAttributeSchema(), mergeInfos, reclaimMap);
        BucketMaps bucketMaps =
            BucketMapCreator::CreateBucketMaps(options.GetMergeConfig().truncateOptionConfig, &attrReaderCreator);
        creator.Init(mSchema, options.GetMergeConfig(), outputSegMergeInfos, reclaimMap, archiveFolder,
                     &attrReaderCreator, &bucketMaps, segDir->GetVersion().GetTimestamp() / (1000 * 1000));

        IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("text");
        config::MergeIOConfig ioConfig;
        return creator.Create(indexConfig, ioConfig);
    }

    std::shared_ptr<PostingIterator> CreatePosting(const std::string& termString,
                                                   const partition::OnlinePartitionReader& partitionReader)
    {
        auto indexReader = partitionReader.GetInvertedIndexReader("text");
        std::shared_ptr<PostingIterator> postIter1(indexReader->Lookup(Term(termString, "text")).ValueOrThrow());
        std::shared_ptr<PostingIterator> postIter2(indexReader->Lookup(Term(termString, "text")).ValueOrThrow());
        TruncatePostingIteratorPtr postIter(new TruncatePostingIterator(postIter1, postIter2));
        assert(postIter);
        return postIter;
    }

private:
    string mRootDir;
    IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateIndexWriterTest);

INDEXLIB_UNIT_TEST_CASE(TruncateIndexWriterTest, TestCaseForSimple);
} // namespace indexlib::index::legacy
