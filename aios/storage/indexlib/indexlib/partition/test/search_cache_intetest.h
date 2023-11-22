#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class SearchCacheTest : public INDEXLIB_TESTBASE
{
public:
    SearchCacheTest();
    ~SearchCacheTest();

    DECLARE_CLASS_NAME(SearchCacheTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestKVSearchWithOnlyInc();
    void TestKVSearchWithIncRtBuilding();
    void TestKVSearchAfterDumpRtSegment();
    void TestKVSearchFromBuilding();
    void TestKVCacheItemBeenInvalidated();
    void TestKVSearchNotFoundKeyInCache();
    void TestKVSearchDeletedKeyInCache();
    void TestKVSearchCacheExpired();
    void TestKVSearchWithTTL();
    void TestKVIncPartlyCoverBuildingSegment();
    void TestKVSearchCacheRtLackSomeDoc();
    void TestKKVSearchWithOnlyInc();
    void TestKKVSearchWithIncRtBuilding();
    void TestKKVSearchAfterDumpRtSegment();
    void TestKKVSearchFromBuilding();

    // inc cover rt
    void TestKKVCacheItemBeenInvalidated();
    void TestKKVIncCoverRtWithCacheItemValid();
    void TestKKVIncCoverAllRt();
    void TestKKVIncCoverAllRtWithBuilding();
    void TestKKVIncPartlyCoverBuildingSegment();

    void TestKKVSearchNotFoundKeyInCache();
    void TestKKVSearchDeletedKeyInCache();
    void TestKKVSearchWithSKey();
    void TestKKVSearchWithTTL();
    void TestKKVSearchBuiltSegmentHasDeletedPKey();
    void TestKKVSearchBuiltSegmentHasDeletedSKey();
    void TestKKVSearchWithTruncateLimits();
    void TestKKVSearchCacheExpired();
    void TestKKVSearchWithKeepSortSequence();

    void TestDirtyCache();
    void TestBug21328825();

private:
    void GetKVValue(test::PartitionStateMachine& psm, const std::string& key, autil::StringView& value,
                    autil::mem_pool::Pool* pool);

    void CheckKKVReader(const index::KKVReaderPtr& kkvReader, autil::StringView& pkeyStr, const std::string& skeyInfos,
                        const std::string& expectResult, TableSearchCacheType type);

private:
    config::IndexPartitionSchemaPtr mKVSchema;
    config::IndexPartitionSchemaPtr mKKVSchema;
    config::IndexPartitionOptions mOptions;
    partition::IndexPartitionResource mResource;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchWithOnlyInc);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchWithIncRtBuilding);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchAfterDumpRtSegment);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchFromBuilding);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVCacheItemBeenInvalidated);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchNotFoundKeyInCache);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchDeletedKeyInCache);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchCacheExpired);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchWithTTL);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVIncPartlyCoverBuildingSegment);

INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchWithOnlyInc);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchWithIncRtBuilding);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchAfterDumpRtSegment);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchFromBuilding);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVCacheItemBeenInvalidated);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVIncCoverRtWithCacheItemValid);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVIncCoverAllRt);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVIncCoverAllRtWithBuilding);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchNotFoundKeyInCache);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchDeletedKeyInCache);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchWithSKey);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchWithTTL);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchBuiltSegmentHasDeletedPKey);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchBuiltSegmentHasDeletedSKey);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchWithTruncateLimits);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchCacheExpired);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVIncPartlyCoverBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKVSearchCacheRtLackSomeDoc);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestKKVSearchWithKeepSortSequence);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestDirtyCache);
INDEXLIB_UNIT_TEST_CASE(SearchCacheTest, TestBug21328825);
}} // namespace indexlib::partition
