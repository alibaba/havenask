#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/document_checker_for_gtest.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/test/index_partition_maker.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace merger {

class MultiPartitionMergerTest : public INDEXLIB_TESTBASE
{
public:
    typedef DocumentMaker::MockIndexPart MockIndexPart;
    typedef vector<string> StringVec;

public:
    DECLARE_CLASS_NAME(MultiPartitionMergerTest);
    void CaseSetUp() override
    {
        RESET_FILE_SYSTEM("", false, FileSystemOptions::Offline());
        mRootDir = GET_TEMP_DATA_PATH();
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                                         "text1:text;text2:text;string1:string;long1:uint32", // field schema
                                         "pack1:pack:text1,text2;pk:primarykey64:string1",    // index schema
                                         "long1;string1",                                     // attribute schema
                                         "string1;long1");                                    // summary schema
        mOptions.SetIsOnline(false);

        mSubSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(
            mSubSchema,
            "sub_text1:text;sub_text2:text;sub_string1:string;sub_long1:uint32",  // field schema
            "sub_pack1:pack:sub_text1,sub_text2;sub_pk:primarykey64:sub_string1", // index schema
            "sub_long1;sub_string1",                                              // attribute schema
            "");                                                                  // summary schema
    }

    void CaseTearDown() override {}

    // for igraph merge
    void TestCaseForIgraph()
    {
        std::string path = "/home/yijie.zhang/index/raw/";
        std::string targetPath = "/home/yijie.zhang/index/index/";
        fslib::FileList dirList;

        file_system::FslibWrapper::ListDirE(path, dirList);

        std::vector<std::string> srcVec;
        for (size_t i = 0; i < dirList.size(); i++) {
            std::string partSrc = util::PathUtil::JoinPath(path, dirList[i]);
            srcVec.push_back(partSrc);
            cout << partSrc << endl;
        }

        file_system::FslibWrapper::DeleteDirE(targetPath, DeleteOption::NoFence(false));
        config::IndexPartitionOptions options;
        options.GetOfflineConfig().fullIndexStoreKKVTs = true;
        // options.GetMergeConfig().mergeIOConfig.readBufferSize = 4 * 1024 * 1024;
        options.GetMergeConfig().mergeIOConfig.enableAsyncRead = true;
        options.GetMergeConfig().mergeThreadCount = 4;
        MultiPartitionMerger multiPartMerger(options, NULL, "", CommonBranchHinterOption::Test());
        multiPartMerger.Merge(srcVec, targetPath);
    }

    void TestCaseForReadMultiPart()
    {
        MockIndexPart mockIndexPart;
        DocumentMaker::DocumentVect docVect;
        mOptions.GetBuildConfig().maxDocCount = 10;

        vector<string> roots;
        int32_t partCount = 2;
        int32_t docCountPart = 15;
        for (int32_t i = 0; i < partCount; i++) {
            stringstream ss;
            ss << mRootDir << "partition_" << i << "/";
            roots.push_back(ss.str());

            MockIndexPart mockSubIndexPart;
            MakeDataByDocCount(ss.str(), docCountPart, mockIndexPart, mockSubIndexPart, docVect, i * docCountPart,
                               false, false);
        }

        config::IndexPartitionOptions options;
        index_base::PartitionDataPtr partitionData =
            partition::OnDiskPartitionData::TEST_CreateOnDiskPartitionData(options, roots, false, true);
        OnlinePartitionReaderPtr reader(
            new OnlinePartitionReader(mOptions, mSchema, util::SearchCachePartitionWrapperPtr()));
        reader->Open(partitionData);

        DocumentCheckerForGtest checker;
        checker.CheckData(reader, mSchema, mockIndexPart);
    }

    void TestCaseForReadSubMultiPart()
    {
        MockIndexPart mockIndexPart;
        MockIndexPart mockSubIndexPart;
        DocumentMaker::DocumentVect docVect;
        mOptions.GetBuildConfig().maxDocCount = 10;
        int32_t partCount = 2;
        int32_t docCountPart = 15;

        vector<string> roots;
        for (int32_t i = 0; i < partCount; i++) {
            stringstream ss;
            ss << mRootDir << "partition_" << i << "/";
            roots.push_back(ss.str());
            MakeDataByDocCount(ss.str(), docCountPart, mockIndexPart, mockSubIndexPart, docVect, i * docCountPart,
                               false, true);
        }

        config::IndexPartitionOptions options;
        index_base::PartitionDataPtr partitionData =
            partition::OnDiskPartitionData::TEST_CreateOnDiskPartitionData(options, roots, true, true);
        file_system::DirectoryPtr dir = GET_CHECK_DIRECTORY()->GetDirectory("partition_0", false);
        IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(dir);
        OnlinePartitionReaderPtr reader(
            new OnlinePartitionReader(mOptions, schema, util::SearchCachePartitionWrapperPtr()));
        reader->Open(partitionData);

        // TODO: check join docid
        DocumentCheckerForGtest checker;
        checker.CheckData(reader, mSchema, mockIndexPart);

        IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
        checker.CheckData(subReader, mSubSchema, mockSubIndexPart);
    }

    void TestCaseForMultiPartMerge() { TestForMultiPartMerge(2, 1, 3, false); }

    void TestCaseForOnePartMerge() { TestForMultiPartMerge(1, 2, 3, false); }

    void TestCaseForOneSegMerge() { TestForMultiPartMerge(1, 1, 3, false); }

    void TestCaseForMultiPartSortMerge() { TestForMultiPartMerge(2, 1, 3, true); }

    void TestCaseForOnePartSortMerge() { TestForMultiPartMerge(1, 2, 3, true); }

    void TestCaseForOneSegSortMerge() { TestForMultiPartMerge(1, 1, 3, false); }

private:
    void TestForMultiPartMerge(int32_t partCount, int32_t segmentCount, int32_t docCountPerSeg, bool sortByWeight)
    {
        int32_t docCountPart = segmentCount * docCountPerSeg;
        AttributeConfigPtr sortAttributeConfig;
        StringVec sortFieldNames;
        StringVec sortPatterns;
        if (sortByWeight) {
            mSchema.reset(new IndexPartitionSchema);
            PartitionSchemaMaker::MakeSchema(
                mSchema,
                "text1:text;text2:text;string1:string;long1:uint32;long2:uint32", // field schema
                "pack1:pack:text1,text2;pk:primarykey64:string1",                 // index schema
                "long1;string1;long2",                                            // attribute schema
                "string1;long1");                                                 // summary schema
            mOptions.GetBuildConfig().maxDocCount = docCountPerSeg;

            sortFieldNames.push_back("long1");
            sortFieldNames.push_back("long2");
            sortPatterns.push_back("DESC");
            sortPatterns.push_back("ASC");
        } else {
            mOptions.GetBuildConfig().maxDocCount = docCountPerSeg;
        }
        MockIndexPart mockIndexPart;
        DocumentMaker::DocumentVect docVect;
        vector<string> roots;

        int32_t pkStartSuffix = 0;
        for (int32_t i = 0; i < partCount; i++) {
            stringstream ss;
            ss << mRootDir << "partition_" << i << "/";
            roots.push_back(ss.str());
            MockIndexPart mockSubIndexPart;
            MakeDataByDocCount(ss.str(), docCountPart, mockIndexPart, sortFieldNames, sortPatterns, docVect,
                               pkStartSuffix);
            pkStartSuffix += docCountPart;
        }
        for (size_t i = 0; i < docVect.size(); ++i) {
            docVect[i]->SetDocId((docid_t)i);
        }
        std::cout << "------------------" << std::endl;
        string mergeTo = mRootDir + "multi_partition/";

        MultiPartitionMerger merger(mOptions, NULL, "", CommonBranchHinterOption::Test());
        merger.Merge(roots, mergeTo);

        std::cout << "--------ddddddddddd----------" << std::endl;
        auto segDir = GET_TEMP_DATA_PATH("multi_partition/segment_0_level_0");
        CheckSegmentInfo(segDir, partCount * segmentCount * docCountPerSeg);

        mOptions.SetIsOnline(true);
        partition::OnlinePartition indexPart;
        indexPart.Open(mergeTo, "", mSchema, mOptions);
        IndexPartitionReaderPtr reader = indexPart.GetReader();
        std::cout << "--------eeeeeeeeeeee----------" << std::endl;
        if (sortByWeight) {
            vector<uint32_t> docCounts;
            for (int i = 0; i < partCount * segmentCount; i++) {
                docCounts.push_back(docCountPerSeg);
            }
            AttributeSchemaPtr attributeSchema = mSchema->GetAttributeSchema();
            std::vector<AttributeConfigPtr> attributeConfigVec;
            AttributeConfigPtr sortAttributeConfig1 = attributeSchema->GetAttributeConfig("long1");
            attributeConfigVec.push_back(sortAttributeConfig1);
            AttributeConfigPtr sortAttributeConfig2 = attributeSchema->GetAttributeConfig("long2");
            attributeConfigVec.push_back(sortAttributeConfig2);
            IndexPartitionMaker::MultiSortDocuments(docCounts, attributeConfigVec, sortPatterns, docVect);
            ReclaimMapPtr reclaimMap = IndexPartitionMaker::MakeSortedReclaimMap(mockIndexPart, docVect);
            MockIndexPart tmpIndexPart;
            DocumentMaker::ReclaimDocuments(mockIndexPart, reclaimMap, tmpIndexPart);

            DocumentCheckerForGtest checker;
            checker.CheckData(reader, mSchema, tmpIndexPart);
        } else {
            DocumentCheckerForGtest checker;
            checker.CheckData(reader, mSchema, mockIndexPart);
        }
    }

    void CheckSegmentInfo(const std::string& segInfoDir, uint32_t docCount)
    {
        SegmentInfo segInfo;
        segInfo.TEST_Load(segInfoDir + "/" + SEGMENT_INFO_FILE_NAME);
        INDEXLIB_TEST_EQUAL(docCount, segInfo.docCount);
        INDEXLIB_TEST_EQUAL(true, segInfo.mergedSegment);
        // todo: check partition meta
    }

    void MakeDataByDocCount(const string& partDir, uint32_t docCount, MockIndexPart& mockIndexPart,
                            MockIndexPart& mockSubIndexPart, DocumentMaker::DocumentVect& retDocVect,
                            int32_t pkStartSuffix = -1, bool needSort = false, bool hasSub = false)
    {
        string sortFieldName = hasSub ? "sub_long1" : "long1";

        string mainDocStr;
        string subDocStr;
        std::cout << "----------begin make docStr--------" << std::endl;
        if (!needSort) {
            mainDocStr = CreateDocString(mSchema, "string1", docCount, pkStartSuffix);
            subDocStr = CreateDocString(mSubSchema, "sub_string1", docCount, pkStartSuffix);
        } else {
            mainDocStr = CreateDocString(mSchema, "string1", docCount, pkStartSuffix, "long1");
            subDocStr = CreateDocString(mSubSchema, "sub_string1", docCount, pkStartSuffix, "sub_long1");
        }
        std::cout << "----------begin make doc--------" << std::endl;
        DocumentMaker::DocumentVect docVect;
        DocumentMaker::MakeDocuments(mainDocStr, mSchema, docVect, mockIndexPart);

        if (hasSub) {
            DocumentMaker::DocumentVect subDocVect;
            DocumentMaker::MakeDocuments(subDocStr, mSubSchema, subDocVect, mockSubIndexPart);
            assert(docVect.size() == subDocVect.size());
            for (size_t i = 0; i < docVect.size(); ++i) {
                docVect[i]->AddSubDocument(subDocVect[i]);
            }
            mSchema->SetSubIndexPartitionSchema(mSubSchema);
        }
        std::cout << "----------end make doc--------" << std::endl;
        util::QuotaControlPtr quotaControl(new util::QuotaControl(1024 * 1024 * 1024));
        IndexBuilder builder(partDir, mOptions, mSchema, quotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(builder.Init());
        std::cout << "------------------" << std::endl;
        for (uint32_t i = 0; i < docCount; ++i) {
            builder.Build(docVect[i]);
            retDocVect.push_back(docVect[i]);
        }
        builder.EndIndex();
        builder.TEST_BranchFSMoveToMainRoad();
    }

    string CreateDocString(IndexPartitionSchemaPtr schema, std::string pkField, uint32_t docCount,
                           int32_t pkStartSuffix = -1, std::string sortFieldName = "")
    {
        string docStr;
        if (sortFieldName.empty()) {
            DocumentMaker::MakeDocumentsStr(docCount, schema->GetFieldSchema(), docStr, pkField, pkStartSuffix);
        } else {
            DocumentMaker::MakeSortedDocumentsStr(docCount, schema->GetFieldSchema(), docStr, sortFieldName, pkField,
                                                  pkStartSuffix);
        }
        return docStr;
    }

    void MakeDataByDocCount(const string& partDir, uint32_t docCount, MockIndexPart& mockIndexPart,
                            const StringVec& sortFieldNames, const StringVec& sortPatterns,
                            DocumentMaker::DocumentVect& retDocVect, int32_t pkStartSuffix = -1)
    {
        string docStr;
        std::cout << "----------begin make docStr--------" << std::endl;
        if (sortFieldNames.empty()) {
            DocumentMaker::MakeDocumentsStr(docCount, mSchema->GetFieldSchema(), docStr, "string1", pkStartSuffix);
        } else {
            DocumentMaker::MakeMultiSortedDocumentsStr(docCount, mSchema->GetFieldSchema(), docStr, sortFieldNames,
                                                       sortPatterns, "string1", pkStartSuffix);
        }
        std::cout << "----------begin make docs--------" << std::endl;
        DocumentMaker::DocumentVect docVect;
        DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);
        std::cout << "----------end make docs--------" << std::endl;
        PartitionMeta partMeta;
        for (size_t i = 0; i < sortFieldNames.size(); i++) {
            partMeta.AddSortDescription(sortFieldNames[i],
                                        indexlibv2::config::SortDescription::SortPatternFromString(sortPatterns[i]));
        }
        partMeta.Store(partDir, FenceContext::NoFence());

        std::cout << "----------begin build--------" << std::endl;
        util::QuotaControlPtr quotaControl(new util::QuotaControl(1024 * 1024 * 1024));
        IndexBuilder builder(partDir, mOptions, mSchema, quotaControl, BuilderBranchHinter::Option::Test());
        INDEXLIB_TEST_TRUE(builder.Init());
        for (uint32_t i = 0; i < docCount; ++i) {
            builder.Build(docVect[i]);
            retDocVect.push_back(docVect[i]);
        }
        std::cout << "----------end build--------" << std::endl;
        builder.EndIndex();
        std::cout << "----------end dump--------" << std::endl;

        // move branch files to main-path
        builder.TEST_BranchFSMoveToMainRoad();
    }

private:
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionSchemaPtr mSubSchema;
    IndexPartitionOptions mOptions;
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForReadMultiPart);
// INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForIgraph);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForReadSubMultiPart);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForOneSegMerge);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForOnePartMerge);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForOnePartSortMerge);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForOneSegSortMerge);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForMultiPartMerge);
INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForMultiPartSortMerge);
// INDEXLIB_UNIT_TEST_CASE(MultiPartitionMergerTest, TestCaseForSortSubPartitions);
}} // namespace indexlib::merger
