#pragma once

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_merger_typed.h"
#include "indexlib/index/normal/primarykey/test/primary_key_test_case_helper.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/HashString.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;

namespace indexlib { namespace index {

class PrimaryKeyIndexMergerTypedTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyIndexMergerTypedTest() {};
    virtual ~PrimaryKeyIndexMergerTypedTest() {};

    DECLARE_CLASS_NAME(PrimaryKeyIndexMergerTypedTest);

public:
    virtual void CaseSetUp() override;
    virtual void CaseTearDown() override;

    virtual void TestCaseForMergeUInt64();
    virtual void TestCaseForMergeUInt64WithDelete();
    virtual void TestCaseForMergeUInt128();
    virtual void TestCaseForMergeUInt128WithDelete();
    virtual void TestCaseForMergeUInt64PKAttr();
    virtual void TestCaseForMergeUInt64WithDeletePKAttr();
    virtual void TestCaseForMergeUInt128PKAttr();
    virtual void TestCaseForMergeUInt128WithDeletePKAttr();
    void TestMergeFromIncontinuousSegments();
    void TestMergeToSegmentWithoutDoc();

protected:
    config::IndexConfigPtr CreateIndexConfig(uint64_t key, const std::string& indexName, bool hasPKAttribute = true);
    config::IndexConfigPtr CreateIndexConfig(autil::uint128_t key, const std::string& indexName,
                                             bool hasPKAttribute = true);
    virtual PrimaryKeyIndexType GetPrimaryKeyIndexType() const { return pk_sort_array; }

private:
    template <typename Key>
    void TestMerge(IndexTestUtil::ToDelete toDel, bool hasPKAttr);

    template <typename Key>
    merger::SegmentDirectoryPtr CreateData2Merge(const string& pksStr, index_base::SegmentInfos& segInfos);

    template <typename Key>
    index_base::SegmentInfo MakeOneSegment(segmentid_t segmentId, const std::string& pksStr);

    template <typename Key>
    void MakeSegmentMergeInfos(const index_base::SegmentInfos& segInfos, const DeletionMapReaderPtr& delReader,
                               index_base::SegmentMergeInfos& segMergeInfos);

    template <typename Key>
    void CheckMergeData(const indexlib::index::LegacyPrimaryKeyReader<Key>& reader, docid_t answer[],
                        uint32_t answerSize);

    ReclaimMapPtr CreateReclaimMap(const index_base::SegmentMergeInfos& segMergeInfos,
                                   const DeletionMapReaderPtr& deletionMapReader);
    DeletionMapReaderPtr CreateDeletionMap(const merger::SegmentDirectoryPtr& segDir,
                                           const index_base::SegmentInfos& segInfos, IndexTestUtil::ToDelete toDel);

    void CreateIndexDocuments(autil::mem_pool::Pool* pool, const std::string& docsStr,
                              std::vector<document::IndexDocumentPtr>& docs);

protected:
    string mRootDir;

private:
    IndexConfigPtr mIndexConfig;
};

template <typename Key>
void PrimaryKeyIndexMergerTypedTest::TestMerge(IndexTestUtil::ToDelete toDel, bool hasPKAttr)
{
    mIndexConfig = CreateIndexConfig((Key)0, "pk", hasPKAttr);
    // prepare build data
    string pksStr("pkstr0:0,pkstr1:1,pkstr2:2;"
                  "pkstr3:0,pkstr4:1,pkstr5:2;pkstr6:0,pkstr7:1,pkstr8:2");
    index_base::SegmentInfos segInfos;
    merger::SegmentDirectoryPtr segDir = CreateData2Merge<Key>(pksStr, segInfos);

    // merge
    PrimaryKeyIndexMergerTyped<Key> merger;
    merger.Init(mIndexConfig);
    merger.BeginMerge(segDir);
    segmentid_t segmentId = segDir->GetVersion().GetLastSegment() + 1;
    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << '_' << segmentId << "_level_0/";
    file_system::DirectoryPtr dir = GET_PARTITION_DIRECTORY()->MakeDirectory(ss.str());
    file_system::DirectoryPtr indexDir = dir->MakeDirectory(INDEX_DIR_NAME);
    DeletionMapReaderPtr deletionMapReader = CreateDeletionMap(segDir, segInfos, toDel);

    index_base::SegmentMergeInfos segMergeInfos;
    MakeSegmentMergeInfos<Key>(segInfos, deletionMapReader, segMergeInfos);

    ReclaimMapPtr reclaimMap = CreateReclaimMap(segMergeInfos, deletionMapReader);
    index::MergerResource resource;
    resource.reclaimMap = reclaimMap;
    resource.targetSegmentCount = 1;
    index_base::OutputSegmentMergeInfo outputInfo;
    outputInfo.path = indexDir->GetLogicalPath();
    outputInfo.directory = indexDir;
    outputInfo.targetSegmentIndex = 0;

    merger.Merge(resource, segMergeInfos, {outputInfo});
    index_base::SegmentInfo segInfo;
    segInfo.docCount = 9 - deletionMapReader->GetDeletedDocCount();
    segInfo.Store(dir);

    // read for check
    index::LegacyPrimaryKeyReader<Key> reader;
    index_base::Version version(1);
    version.AddSegment(segmentId);
    version.Store(GET_PARTITION_DIRECTORY(), false);

    index_base::PartitionDataPtr partitionData = test::PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());

    // always no delete after merge.
    ASSERT_NO_THROW(reader.Open(mIndexConfig, partitionData));
    if (toDel == IndexTestUtil::NoDelete) {
        docid_t answer[] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
        uint32_t answerSize = sizeof(answer) / sizeof(answer[0]);
        CheckMergeData(reader, answer, answerSize);
    } else if (toDel == IndexTestUtil::DeleteEven) {
        docid_t answer[] = {-1, 0, -1, 1, -1, 2, -1, 3, -1};
        uint32_t answerSize = sizeof(answer) / sizeof(answer[0]);
        CheckMergeData(reader, answer, answerSize);
    } else {
        assert(false);
    }
}

template <typename Key>
merger::SegmentDirectoryPtr PrimaryKeyIndexMergerTypedTest::CreateData2Merge(const string& pksStr,
                                                                             index_base::SegmentInfos& segInfos)
{
    StringTokenizer st(pksStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    index_base::Version version(0);
    segmentid_t segmentId = 0;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        index_base::SegmentInfo segInfo = MakeOneSegment<Key>(segmentId, st[i]);
        segInfos.push_back(segInfo);
        version.AddSegment(segmentId);
        segmentId++;
    }

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false);
    return segDir;
}

template <typename Key>
index_base::SegmentInfo PrimaryKeyIndexMergerTypedTest::MakeOneSegment(segmentid_t segmentId, const std::string& pksStr)
{
    file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    file_system::DirectoryPtr segmentDirectory = MAKE_SEGMENT_DIRECTORY(segmentId);
    file_system::DirectoryPtr indexDirectory = segmentDirectory->MakeDirectory(INDEX_DIR_NAME);

    autil::mem_pool::Pool pool;
    PrimaryKeyWriterTyped<Key> pkWriter;
    pkWriter.Init(mIndexConfig, NULL);

    std::vector<document::IndexDocumentPtr> indexDocuments;
    CreateIndexDocuments(&pool, pksStr, indexDocuments);
    for (size_t i = 0; i < indexDocuments.size(); i++) {
        pkWriter.EndDocument(*indexDocuments[i]);
    }

    pkWriter.Dump(indexDirectory, &pool);
    index_base::SegmentInfo segInfo;
    segInfo.docCount = indexDocuments.size();
    segInfo.Store(segmentDirectory);
    indexDocuments.clear();
    GET_FILE_SYSTEM()->Sync(true).GetOrThrow();
    return segInfo;
}

template <typename Key>
void PrimaryKeyIndexMergerTypedTest::MakeSegmentMergeInfos(const index_base::SegmentInfos& segInfos,
                                                           const DeletionMapReaderPtr& delReader,
                                                           index_base::SegmentMergeInfos& segMergeInfos)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < segInfos.size(); ++i) {
        index_base::SegmentMergeInfo segMergeInfo((segmentid_t)i, segInfos[i],
                                                  delReader->GetDeletedDocCount((segmentid_t)i), baseDocId);
        baseDocId += segInfos[i].docCount;
        segMergeInfos.push_back(segMergeInfo);
    }
}

template <typename Key>
void PrimaryKeyIndexMergerTypedTest::CheckMergeData(const indexlib::index::LegacyPrimaryKeyReader<Key>& reader,
                                                    docid_t answer[], uint32_t answerSize)
{
    AttributeReaderPtr pkAttrReader = reader.GetLegacyPKAttributeReader();
    config::SingleFieldIndexConfigPtr singleFieldConfig =
        DYNAMIC_POINTER_CAST(config::SingleFieldIndexConfig, mIndexConfig);
    bool hasPKAttr = singleFieldConfig->HasPrimaryKeyAttribute();
    if (hasPKAttr) {
        INDEXLIB_TEST_TRUE(pkAttrReader);
    } else {
        INDEXLIB_TEST_TRUE(!pkAttrReader);
    }
    for (uint32_t i = 0; i < answerSize; ++i) {
        stringstream ss;
        ss << "pkstr" << i;
        INDEXLIB_TEST_EQUAL(answer[i], reader.Lookup(ss.str(), nullptr));

        if (answer[i] != INVALID_DOCID && hasPKAttr) {
            string strKey;
            pkAttrReader->Read(answer[i], strKey);
            util::HashString hashFunc;
            Key expectHashKey;
            hashFunc.Hash(expectHashKey, ss.str().c_str(), ss.str().size());
            stringstream ss;
            ss << expectHashKey;
            INDEXLIB_TEST_EQUAL(ss.str(), strKey);
        }
    }
}
}} // namespace indexlib::index
