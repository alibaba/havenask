#include "sql/data/TableType.h"

#include <cstddef>
#include <cstdint>
#include <engine/TypeContext.h>
#include <memory>
#include <string>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/util/NaviTestPool.h"
#include "sql/data/TableData.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace table;
using namespace autil;
using namespace navi;

namespace sql {

class TableTypeTest : public TESTBASE {
public:
    TableTypeTest();

public:
    void setUp() override;
    void tearDown() override;

public:
    autil::mem_pool::PoolPtr _poolPtr;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(data, TableTypeTest);

TableTypeTest::TableTypeTest()
    : _poolPtr(new autil::mem_pool::PoolAsan) {}

void TableTypeTest::setUp() {}

void TableTypeTest::tearDown() {}

TEST_F(TableTypeTest, testSimple) {
    TablePtr table;
    {
        autil::mem_pool::PoolPtr tablePool(new autil::mem_pool::PoolAsan);
        table.reset(new Table(tablePool));
    }

    auto mcColumn = TableUtil::declareAndGetColumnData<autil::MultiChar>(table, "multi_char_col");
    auto intColumn = TableUtil::declareAndGetColumnData<int>(table, "int_col");
    for (size_t i = 0; i < 3; ++i) {
        MultiChar mc;
        {
            string data = std::to_string(i);
            char *mcBuf = autil::MultiValueCreator::createMultiValueBuffer<char>(
                data.c_str(), data.size(), _poolPtr.get());
            mc.init(mcBuf);
        }
        Row tableRow = table->allocateRow();
        mcColumn->set(tableRow, mc);
        intColumn->set(tableRow, i);
    }

    string rawStr;
    {
        DataPtr data;
        data = TableDataPtr(new TableData(table));

        DataBuffer dataBuffer;
        TypeContext ctx(dataBuffer);
        TableType tp;
        ASSERT_EQ(navi::TEC_NONE, tp.serialize(ctx, data));
        rawStr.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    ASSERT_LT(0, rawStr.size());
    DataPtr outputData;
    {
        DataBuffer dataBuffer((void *)rawStr.data(), rawStr.size(), _poolPtr.get());
        autil::mem_pool::PoolPtr tablePool(new autil::mem_pool::PoolAsan);
        TypeContext ctx(dataBuffer);
        TableType tp;
        ASSERT_EQ(navi::TEC_NONE, tp.deserialize(ctx, outputData));
    }

    auto *tableData = dynamic_cast<TableData *>(outputData.get());
    ASSERT_NE(nullptr, tableData);
    auto outputTable = tableData->getTable();
    ASSERT_NE(nullptr, outputTable);

    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputColumn<int32_t>(outputTable, "int_col", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputColumn(outputTable, "multi_char_col", {"0", "1", "2"}));
}

} // end namespace sql
