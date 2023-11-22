#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PrimaryKeyIndexReaderTypedIntetestTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    PrimaryKeyIndexReaderTypedIntetestTest();
    ~PrimaryKeyIndexReaderTypedIntetestTest();

    DECLARE_CLASS_NAME(PrimaryKeyIndexReaderTypedIntetestTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestPkValidInPrevSegment();
    void TestLookup();
    void TestLookupRealtimeSegment();
    void TestLookupIncAndRtSegment();
    void TestPkSegmentCombineForRead_1();
    void TestPkSegmentCombineForRead_2();
    void TestPkSegmentCombineForRead_3();
    void TestEstimateLoadSize();
    void TestEstimateLoadSizeWithPkSegmentMerge();
    void TestLookupForLastDocId();
    void TestBuildAndLoadHashPk();
    void TestLoadPkWithLoadConfig();
    void TestPkWithMurmurHash();
    void TestMultiInMemSegments();

private:
    void DoTestPkSegmentCombineForRead(PrimaryKeyIndexType type);
    void DoTestLookupForLastDocId(PrimaryKeyIndexType type);
    void DoTestBuildAndLoadHashPk(InvertedIndexType type);
    template <typename Key>
    bool CheckLookup(PrimaryKeyIndexReaderTyped<Key>& reader, const std::string& key, const docid_t expectDocId,
                     const docid_t expectLastDocId, bool isNumberHash = false);
    void InnerTestCombineSegmentForRead(const std::string& offlineBuildDocs, const std::string& rtBuildDocs,
                                        const std::string& speedUpParam, const std::string& expectResults,
                                        PrimaryKeyIndexType type, size_t expectSegmentNum);
    void InnerTestEstimateLoadSize(const std::string& docStr, const std::string& newDocStr, bool isPkHash64,
                                   bool hasPkAttribute, PrimaryKeyIndexType type, bool loadWithPkHash,
                                   size_t expectSize, bool isLock = true);
    void InnerTestLoadPkWithLoadConfig(PrimaryKeyIndexType type, file_system::LoadConfigList loadConfigList,
                                       bool testGetMmapNonlockFileLength = false);
    void RewriteLoadConfig(config::IndexPartitionOptions& options, bool isLock);

    void CheckPkAttribute(const AttributeReaderPtr& attrReader, docid_t docId, const std::string& pkStr,
                          const config::PrimaryKeyIndexConfigPtr& pkConfig);

private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestEstimateLoadSizeWithPkSegmentMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestEstimateLoadSize);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestPkValidInPrevSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestLookup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestLookupRealtimeSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestLookupIncAndRtSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestPkSegmentCombineForRead_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestPkSegmentCombineForRead_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestPkSegmentCombineForRead_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestLookupForLastDocId);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestBuildAndLoadHashPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestLoadPkWithLoadConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestPkWithMurmurHash);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PrimaryKeyIndexReaderTypedIntetestTest, TestMultiInMemSegments);

INSTANTIATE_TEST_CASE_P(BuildMode, PrimaryKeyIndexReaderTypedIntetestTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index
