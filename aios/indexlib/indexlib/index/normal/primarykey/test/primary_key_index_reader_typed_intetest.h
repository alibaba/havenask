#ifndef __INDEXLIB_PRIMARYKEYINDEXREADERTYPEDINTETESTTEST_H
#define __INDEXLIB_PRIMARYKEYINDEXREADERTYPEDINTETESTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_builder.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyIndexReaderTypedIntetestTest : public INDEXLIB_TESTBASE
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
    void TestPkSegmentCombineForRead();
    void TestEstimateLoadSize();
    void TestEstimateLoadSizeWithPkSegmentMerge();
    void TestLookupForLastDocId();
    void TestBuildAndLoadHashPk();
    void TestLoadHashPkWithLoadConfig();
    void TestPkWithMurmurHash();
    void TestMultiInMemSegments();

private:
    void DoTestPkSegmentCombineForRead(PrimaryKeyIndexType type);
    void DoTestLookupForLastDocId(PrimaryKeyIndexType type);
    void DoTestBuildAndLoadHashPk(IndexType type);
    template<typename Key>
    bool CheckLookup(PrimaryKeyIndexReaderTyped<Key>& reader, 
                     const std::string& key, const docid_t expectDocId,
                     const docid_t expectLastDocId, bool isNumberHash = false);
    void InnerTestCombineSegmentForRead(
            const std::string& offlineBuildDocs, const std::string& rtBuildDocs,
            const std::string& speedUpParam, const std::string& expectResults,
            PrimaryKeyIndexType type, size_t expectSegmentNum);
    void InnerTestEstimateLoadSize(const std::string& docStr,
                                   const std::string& newDocStr,
                                   bool isPkHash64,
                                   bool hasPkAttribute, 
                                   PrimaryKeyIndexType type,
                                   bool loadWithPkHash, size_t expectSize,
                                   bool isLock = true);
    void RewriteLoadConfig(config::IndexPartitionOptions& options, bool isLock);

    void CheckPkAttribute(const AttributeReaderPtr& attrReader,
                          docid_t docId, const std::string& pkStr,
                          const config::PrimaryKeyIndexConfigPtr& pkConfig);

private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestEstimateLoadSizeWithPkSegmentMerge);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestEstimateLoadSize);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestPkValidInPrevSegment);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestLookup);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestLookupRealtimeSegment);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestLookupIncAndRtSegment);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestPkSegmentCombineForRead);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestLookupForLastDocId);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestBuildAndLoadHashPk);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestLoadHashPkWithLoadConfig);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestPkWithMurmurHash);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedIntetestTest, TestMultiInMemSegments);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARYKEYINDEXREADERTYPEDINTETESTTEST_H
