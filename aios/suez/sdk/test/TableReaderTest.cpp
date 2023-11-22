#include "suez/sdk/TableReader.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <set>
#include <string>
#include <vector>

#include "build_service/util/LocatorUtil.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/testlib/mock_index_partition.h"
#include "suez/common/InnerDef.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/sdk/SuezIndexPartitionData.h"
#include "suez/sdk/SuezTabletPartitionData.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/SuezPartitionFactory.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TableReaderTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    MultiTableReader _multiReader;
    PartitionId _pid1;
    PartitionId _pid2;
    PartitionId _pid3;
};

void TableReaderTest::setUp() {
    _pid1.tableId.fullVersion = 1;
    _pid1.tableId.tableName = "table1";
    _pid1.from = 0;
    _pid1.to = 65535;
    _pid1.index = 1;
    _pid2.tableId.fullVersion = 2;
    _pid2.tableId.tableName = "table1";
    _pid2.from = 0;
    _pid2.to = 65535;
    _pid2.index = 1;
    _pid3.tableId.fullVersion = 2;
    _pid3.tableId.tableName = "table2";
    _pid3.from = 0;
    _pid3.to = 65535;
    _pid3.index = 3;

    SuezPartitionFactory factory;

    TableResource tableResource1(_pid1);
    TableResource tableResource2(_pid2);
    TableResource tableResource3(_pid3);
    CurrentPartitionMetaPtr partitionMeta(new PartitionMeta());
    SuezIndexPartitionDataPtr data1 = std::make_shared<SuezIndexPartitionData>(_pid1, 1, nullptr, true);
    SuezIndexPartitionDataPtr data2 = std::make_shared<SuezIndexPartitionData>(_pid2, 1, nullptr, true);
    SuezIndexPartitionDataPtr data3 = std::make_shared<SuezIndexPartitionData>(_pid3, 1, nullptr, true);

    SingleTableReaderPtr singleReader1(new SingleTableReader(data1));
    SingleTableReaderPtr singleReader2(new SingleTableReader(data2));
    SingleTableReaderPtr singleReader3(new SingleTableReader(data3));
    _multiReader.addTableReader(_pid1, singleReader1);
    _multiReader.addTableReader(_pid2, singleReader2);
    _multiReader.addTableReader(_pid3, singleReader3);
}

void TableReaderTest::tearDown() {}

TEST_F(TableReaderTest, testGetTableReader) {
    ASSERT_TRUE(_multiReader.getTableReader(_pid1));
    ASSERT_TRUE(_multiReader.getTableReader(_pid2));
    PartitionId pid3 = _pid2;
    pid3.tableId.tableName = "not exist";
    ASSERT_FALSE(_multiReader.getTableReader(pid3));
}

TEST_F(TableReaderTest, testGetTableReadersByTableName) {
    SingleTableReaderMap readers = _multiReader.getTableReaders("table1");
    ASSERT_EQ(2, readers.size());
    ASSERT_TRUE(readers.find(_pid1) != readers.end());
    ASSERT_TRUE(readers.find(_pid2) != readers.end());
    readers = _multiReader.getTableReaders("not_exist");
    ASSERT_TRUE(readers.empty());
}

TEST_F(TableReaderTest, testErase) {
    ASSERT_TRUE(_multiReader.getTableReader(_pid1));
    ASSERT_TRUE(_multiReader.getTableReader(_pid2));
    _multiReader.erase(_pid1);
    SingleTableReaderMap readers = _multiReader.getTableReaders("table1");
    ASSERT_EQ(1, readers.size());
    ASSERT_FALSE(_multiReader.getTableReader(_pid1));
    ASSERT_TRUE(_multiReader.getTableReader(_pid2));

    PartitionId pid3 = _pid2;
    pid3.tableId.tableName = "not exist";
    _multiReader.erase(pid3);
    readers = _multiReader.getTableReaders("table1");
    ASSERT_EQ(1, readers.size());
    ASSERT_TRUE(_multiReader.getTableReader(_pid2));
    _multiReader.erase(_pid2);
    _multiReader.erase(_pid3);
    ASSERT_EQ(0, _multiReader._namedTableReaders.size());
}

TEST_F(TableReaderTest, getGetTableNameOnlySet) {
    set<string> nameSet = _multiReader.getTableNameOnlySet();
    ASSERT_EQ(2u, nameSet.size());
    ASSERT_EQ("table1", *nameSet.begin());
    ASSERT_EQ("table2", *(++nameSet.begin()));
}

TEST_F(TableReaderTest, testGetPartitionIds) {
    auto pids = _multiReader.getPartitionIds("table1");
    ASSERT_EQ(2u, pids.size());
    pids = _multiReader.getPartitionIds("table2");
    ASSERT_EQ(1u, pids.size());
    pids = _multiReader.getPartitionIds("table3");
    ASSERT_TRUE(pids.empty());
}

TEST_F(TableReaderTest, testGetTabletByIdx) {
    PartitionId pid = TableMetaUtil::makePidFromStr("table4_1");
    pid.index = 1;

    auto tablet = std::make_shared<indexlibv2::framework::NiceMockTablet>();
    auto data = std::make_shared<SuezTabletPartitionData>(pid, 1, tablet, true);
    auto singleReader = std::make_shared<SingleTableReader>(data);
    _multiReader.addTableReader(pid, singleReader);

    ASSERT_TRUE(_multiReader.getTabletByIdx("not exist", 1) == nullptr);
    ASSERT_TRUE(_multiReader.getTabletByIdx("table4", 0) == nullptr);
    ASSERT_TRUE(_multiReader.getTabletByIdx("table1", 1) == nullptr);
    ASSERT_TRUE(_multiReader.getTabletByIdx("table4", 1) != nullptr);
    ASSERT_EQ(_multiReader.getTabletByIdx("table4", 1).get(), tablet.get());
}

TEST_F(TableReaderTest, testGetDataTimestamp) {

    MultiTableReader multiReader;

    // partition
    auto mockReader = new indexlib::testlib::MockIndexPartitionReader();
    ON_CALL(*mockReader, GetLatestDataTimestamp()).WillByDefault(Return(10000));
    indexlib::partition::IndexPartitionResource resource;
    resource.taskScheduler.reset(new indexlib::util::TaskScheduler);
    resource.memoryQuotaController.reset(new indexlibv2::MemoryQuotaController(100));
    auto mockPartition = new indexlib::testlib::MockIndexPartition(resource);
    indexlib::partition::IndexPartitionPtr partitionPtr(mockPartition);
    indexlib::partition::IndexPartitionReaderPtr readerPtr(mockReader);
    ON_CALL(*mockPartition, GetReader()).WillByDefault(Return(readerPtr));

    TableResource tableResource1(_pid1);
    SuezIndexPartitionDataPtr data1 = std::make_shared<SuezIndexPartitionData>(_pid1, 1, partitionPtr, true);
    SingleTableReaderPtr singleReader1(new SingleTableReader(data1));
    multiReader.addTableReader(_pid1, singleReader1);

    int64_t dataTs = -1;
    ASSERT_TRUE(multiReader.getDataTimestamp("table1", dataTs));
    ASSERT_EQ(10000, dataTs);

    // tablet

    auto mockTablet = new indexlibv2::framework::MockTablet;
    std::shared_ptr<indexlibv2::framework::ITablet> tabletPtr(mockTablet);
    indexlibv2::framework::TabletInfos tabletInfo;
    int64_t offset = 0;
    ASSERT_TRUE(build_service::util::LocatorUtil::encodeOffset(0, 500, &offset));
    indexlibv2::framework::Locator locator(0, offset);
    tabletInfo.SetBuildLocator(locator);
    ON_CALL(*mockTablet, GetTabletInfos()).WillByDefault(Return(&tabletInfo));
    dataTs = -1;
    TableResource tableResource2(_pid3);
    auto data2 = std::make_shared<SuezTabletPartitionData>(_pid3, 2, tabletPtr, true);
    SingleTableReaderPtr singleReader2(new SingleTableReader(data2));
    multiReader.addTableReader(_pid3, singleReader2);

    ASSERT_TRUE(multiReader.getDataTimestamp("table2", dataTs));
    ASSERT_EQ(500, dataTs);

    ASSERT_FALSE(multiReader.getDataTimestamp("not_Exist_table", dataTs));
}

} // namespace suez
