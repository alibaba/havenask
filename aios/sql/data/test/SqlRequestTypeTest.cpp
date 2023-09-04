#include "sql/data/SqlRequestType.h"

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
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace navi;

namespace sql {

class SqlRequestTypeTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(data, SqlRequestTypeTest);

void SqlRequestTypeTest::setUp() {}

void SqlRequestTypeTest::tearDown() {}

TEST_F(SqlRequestTypeTest, testSimple) {
    SqlQueryRequestPtr sqlQueryRequest(new SqlQueryRequest());
    sqlQueryRequest->setSqlQueryStr("aaaaa");
    sqlQueryRequest->setSqlParams({{"a", "a1"}, {"b", "b1"}});
    DataPtr sqlRequestData(new SqlRequestData(sqlQueryRequest));
    autil::mem_pool::PoolPtr pool(new autil::mem_pool::PoolAsan);
    DataBuffer dataBuffer(2048, pool.get());
    TypeContext ctx(dataBuffer);
    SqlRequestType type;
    ASSERT_EQ(navi::TEC_NONE, type.serialize(ctx, sqlRequestData));
    DataPtr data;
    ASSERT_EQ(navi::TEC_NONE, type.deserialize(ctx, data));
    auto sqlRequestData2 = dynamic_pointer_cast<SqlRequestData>(data);
    ASSERT_TRUE(sqlRequestData2 != nullptr);
    auto sqlQueryRequest2 = sqlRequestData2->getSqlRequest();
    ASSERT_TRUE(*sqlQueryRequest == *sqlQueryRequest2);
}

} // namespace sql
