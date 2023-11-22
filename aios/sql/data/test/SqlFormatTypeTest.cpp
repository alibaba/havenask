#include "sql/data/SqlFormatType.h"

#include <algorithm>
#include <engine/TypeContext.h>
#include <iosfwd>
#include <memory>
#include <string>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/util/NaviTestPool.h"
#include "sql/data/SqlFormatData.h"
#include "sql/data/SqlQueryRequest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace navi;

namespace sql {

class SqlFormatTypeTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(data, SqlFormatTypeTest);

void SqlFormatTypeTest::setUp() {}

void SqlFormatTypeTest::tearDown() {}

TEST_F(SqlFormatTypeTest, testFormatKeyWord1) {
    SqlQueryRequestPtr sqlQueryRequest(new SqlQueryRequest());
    sqlQueryRequest->setSqlQueryStr("aaaaa");
    const string format1 = "format1";
    sqlQueryRequest->setSqlParams({{SQL_FORMAT_TYPE, format1}});
    DataPtr sqlFormatData(new SqlFormatData(sqlQueryRequest.get()));
    autil::mem_pool::PoolPtr pool(new autil::mem_pool::PoolAsan);
    DataBuffer dataBuffer(2048, pool.get());
    TypeContext ctx(dataBuffer);
    SqlFormatType type;
    ASSERT_EQ(navi::TEC_NONE, type.serialize(ctx, sqlFormatData));
    DataPtr data;
    ASSERT_EQ(navi::TEC_NONE, type.deserialize(ctx, data));
    auto sqlFormatData2 = dynamic_pointer_cast<SqlFormatData>(data);
    ASSERT_TRUE(sqlFormatData2 != nullptr);
    auto sqlQueryFormat2 = sqlFormatData2->getSqlFormat();
    ASSERT_TRUE(sqlQueryFormat2 == format1);
}

TEST_F(SqlFormatTypeTest, testFormatKeyWord2) {
    SqlQueryRequestPtr sqlQueryRequest(new SqlQueryRequest());
    sqlQueryRequest->setSqlQueryStr("aaaaa");
    const string format2 = "format2";
    sqlQueryRequest->setSqlParams({{SQL_FORMAT_TYPE_NEW, format2}});
    DataPtr sqlFormatData(new SqlFormatData(sqlQueryRequest.get()));
    autil::mem_pool::PoolPtr pool(new autil::mem_pool::PoolAsan);
    DataBuffer dataBuffer(2048, pool.get());
    TypeContext ctx(dataBuffer);
    SqlFormatType type;
    ASSERT_EQ(navi::TEC_NONE, type.serialize(ctx, sqlFormatData));
    DataPtr data;
    ASSERT_EQ(navi::TEC_NONE, type.deserialize(ctx, data));
    auto sqlFormatData2 = dynamic_pointer_cast<SqlFormatData>(data);
    ASSERT_TRUE(sqlFormatData2 != nullptr);
    auto sqlQueryFormat2 = sqlFormatData2->getSqlFormat();
    ASSERT_TRUE(sqlQueryFormat2 == format2);
}

} // namespace sql
