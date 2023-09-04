#ifndef __INDEXLIB_TABLEREADERCONTAINERUPDATERTEST_H
#define __INDEXLIB_TABLEREADERCONTAINERUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class TableReaderContainerUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    TableReaderContainerUpdaterTest();
    ~TableReaderContainerUpdaterTest();

    DECLARE_CLASS_NAME(TableReaderContainerUpdaterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestUpdatePartition();
    void TestSwitchUpdater();
    void TestInitPartitions();

private:
    void PepareSchema(size_t schemaCount);
    void AddSub(std::vector<size_t> subIdx);
    IndexPartitionPtr GetIndexPartition(std::string name);
    IndexPartitionReaderPtr GetIndexPartitionReader(std::string name);
    void ErasePartition(std::vector<IndexPartitionPtr>& indexPartitions, std::string name);

private:
    config::IndexPartitionOptions mOptions;
    std::vector<config::IndexPartitionSchemaPtr> mSchemas;
    std::vector<IndexPartitionReaderPtr> mReaders;
    std::vector<IndexPartitionPtr> mIndexPartitions;
    std::unordered_map<std::string, uint32_t> mTableName2PartitionIdMap;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TableReaderContainerUpdaterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TableReaderContainerUpdaterTest, TestUpdatePartition);
INDEXLIB_UNIT_TEST_CASE(TableReaderContainerUpdaterTest, TestSwitchUpdater);
INDEXLIB_UNIT_TEST_CASE(TableReaderContainerUpdaterTest, TestInitPartitions);
}} // namespace indexlib::partition

#endif //__INDEXLIB_TABLEREADERCONTAINERUPDATERTEST_H
