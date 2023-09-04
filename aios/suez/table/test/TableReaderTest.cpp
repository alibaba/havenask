#include "suez/sdk/TableReader.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <set>
#include <string>
#include <vector>

#include "suez/common/InnerDef.h"
#include "suez/sdk/SuezIndexPartitionData.h"
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
    _pid2.tableId.fullVersion = 2;
    _pid2.tableId.tableName = "table1";
    _pid2.from = 0;
    _pid2.to = 65535;
    _pid3.tableId.fullVersion = 2;
    _pid3.tableId.tableName = "table2";
    _pid3.from = 0;
    _pid3.to = 65535;

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

} // namespace suez
