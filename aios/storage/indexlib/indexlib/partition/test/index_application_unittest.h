#pragma once

#include "future_lite/coro/Lazy.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class IndexApplicationTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    IndexApplicationTest();
    ~IndexApplicationTest();

    DECLARE_CLASS_NAME(IndexApplicationTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void AddSub();
    void TestAddIndexPartitions();
    void TestAddIndexPartitionsWithSubDoc();
    void TestParseJoinRelations();
    void TestCreateSnapshot();
    void TestIsLatestSnapshotReader();
    void TestCreateSnapshotForCustomOnlinePartition();
    void TestCreateSnapshotWithCache();
    void TestCreateSnapshotWithMainSubIdx();
    void TestInitByTablet();
    void TestInitByTabletFailed();
    void TestCreateSnapshot4SingleTablet();
    void TestAddIndexPartitionsAndTablets();
    void TestCreateSnapshot4PartitionAndTablet();
    void TestParseJoinRelationsWithTablet();
    void TestUpdateExpiredSnapshotReader();

private:
    std::shared_ptr<indexlibv2::config::TabletSchema> GetDefaultTabletSchema(const std::string& schemaName,
                                                                             const std::string& fieldStr,
                                                                             const std::string& indexStr,
                                                                             const std::string& attrStr);
    std::shared_ptr<indexlibv2::framework::Tablet>
    GetDefaultTablet(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema);

private:
    config::IndexPartitionSchemaPtr schemaA;
    config::IndexPartitionSchemaPtr schemaB;
    config::IndexPartitionSchemaPtr schemaC;
    config::IndexPartitionSchemaPtr schemaD;
    config::IndexPartitionSchemaPtr schemaE;
    IndexPartitionReaderPtr readerA;
    IndexPartitionReaderPtr readerB;
    IndexPartitionReaderPtr readerC;
    IndexPartitionReaderPtr readerD;
    IndexPartitionReaderPtr readerE;
    std::unique_ptr<future_lite::Executor> _executor = nullptr;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler = nullptr;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, IndexApplicationTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestAddIndexPartitions);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestUpdateExpiredSnapshotReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestAddIndexPartitionsWithSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestParseJoinRelations);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestCreateSnapshot);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestIsLatestSnapshotReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestCreateSnapshotForCustomOnlinePartition);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestCreateSnapshotWithCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestCreateSnapshotWithMainSubIdx);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestInitByTablet);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestInitByTabletFailed);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestCreateSnapshot4SingleTablet);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestAddIndexPartitionsAndTablets);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestCreateSnapshot4PartitionAndTablet);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexApplicationTest, TestParseJoinRelationsWithTablet);
}} // namespace indexlib::partition
