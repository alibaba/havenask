#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/archive_folder.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class TruncateIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    typedef DocumentMaker::MockIndexPart MockIndexPart;
public:
    DECLARE_CLASS_NAME(TruncateIndexWriterTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "price:float;user_id:long;text1:text", // above is field schema
                "text:text:text1;", // above is index schema
                "price;user_id", // above is attribute schema
                "price" );// above is summary schema
        
        string docStr = "{ 1, "                                         \
                        "[text1: (token1^3 token2) (token2^2)],"        \
                        "[price: (1.5)],"                               \
                        "[user_id: (100)],"                             \
                        "};";
        DocumentMaker::DocumentVect docVect;
        MockIndexPart mockIndexPart;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        PartitionDataPtr partitionData = 
            PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), mSchema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                container);
        partition::OfflinePartitionWriter writer(options, container);
        writer.Open(mSchema, partitionData, PartitionModifierPtr());
        writer.AddDocument(docVect[0]);
        writer.Close();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
        ArchiveFolderPtr archiveFolder(new ArchiveFolder(true));
        storage::FileSystemWrapper::MkDirIfNotExist(
                PathUtil::JoinPath(mRootDir, 
                        TRUNCATE_META_DIR_NAME));
        archiveFolder->Open(PathUtil::JoinPath(mRootDir,
                        TRUNCATE_META_DIR_NAME));
        TruncateIndexWriterPtr indexWriter = CreateIndexWriter(archiveFolder);
        INDEXLIB_TEST_TRUE(indexWriter.get() != NULL);
        IndexPartitionOptions options;
        partition::OnlinePartitionReader partitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr());
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        PartitionDataPtr partitionData = 
            PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), mSchema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), container);
        partitionReader.Open(partitionData);
        PostingIteratorPtr postIter = CreatePosting("token2", partitionReader);
        indexWriter->AddPosting(100, postIter);
        indexWriter->EndPosting();
        archiveFolder->Close();
    }


private:
    TruncateIndexWriterPtr CreateIndexWriter(const ArchiveFolderPtr& archiveFolder)
    {
        TruncateParams param("1:1", "::", "text=text_price:price#ASC:::");
        IndexPartitionOptions options;
        options.GetMergeConfig().truncateOptionConfig =
            TruncateConfigMaker::MakeConfig(param.truncCommonStr, 
                    param.distinctStr, param.truncIndexConfigStr);
        SegmentMergeInfos mergeInfos;
        segmentid_t segId = 0;
        SegmentInfo segInfo;
        segInfo.docCount = 1;
        segInfo.locator.SetLocator(StringUtil::toString(segId));
        SegmentMergeInfo segMergeInfo(segId, segInfo, 0, 0);
        mergeInfos.push_back(segMergeInfo);

        vector<uint32_t> docCounts;
        docCounts.push_back(1);
        vector<set<docid_t> > delDocIdSets(1);
        ReclaimMapPtr reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, 
                delDocIdSets, true);
        string mergedDir = mRootDir;

        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(mRootDir, 1);
        segDir->Init(false);
        index_base::OutputSegmentMergeInfo outputSegMergeInfo;
        index_base::OutputSegmentMergeInfos outputSegMergeInfos;
        outputSegMergeInfo.path = PathUtil::JoinPath(mRootDir, "index");
        outputSegMergeInfo.directory = file_system::DirectoryCreator::Create(outputSegMergeInfo.path);
        outputSegMergeInfos.push_back(outputSegMergeInfo);
        TruncateIndexWriterCreator creator;
        TruncateAttributeReaderCreator attrReaderCreator(
                segDir, mSchema->GetAttributeSchema(),
                mergeInfos, reclaimMap);
        BucketMaps bucketMaps = BucketMapCreator::CreateBucketMaps(
            options.GetMergeConfig().truncateOptionConfig,
            &attrReaderCreator);
        creator.Init(mSchema, options.GetMergeConfig(), outputSegMergeInfos,
                     reclaimMap, archiveFolder,
                     &attrReaderCreator, &bucketMaps,
                     segDir->GetVersion().GetTimestamp() / (1000 * 1000));
        
        IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("text");
        config::MergeIOConfig ioConfig;
        return creator.Create(indexConfig, ioConfig);
    }

    PostingIteratorPtr CreatePosting(
            const std::string& termString, const partition::OnlinePartitionReader& partitionReader)
    {
        IndexReaderPtr indexReader = partitionReader.GetIndexReader("text");
        PostingIteratorPtr postIter1(indexReader->Lookup(Term(termString, "text")));
        PostingIteratorPtr postIter2(indexReader->Lookup(Term(termString, "text")));
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

IE_NAMESPACE_END(index);
