#include "sql/data/SqlQueryConfigData.h"

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "navi/engine/TypeContext.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace navi;
using namespace autil;

namespace sql {

class SqlQueryConfigDataTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(data, SqlQueryConfigDataTest);

void SqlQueryConfigDataTest::setUp() {}

void SqlQueryConfigDataTest::tearDown() {}

TEST_F(SqlQueryConfigDataTest, testSerialize_PerMember) {
    SqlQueryConfigType type;

    mem_pool::PoolPtr pool(new mem_pool::PoolAsan);
    DataBuffer dataBuffer(2048, pool.get());
    {
        SqlQueryConfig config;
        config.set_resultallowsoftfailure(true);
        auto data = std::make_shared<SqlQueryConfigData>(std::move(config));
        navi::TypeContext ctx(dataBuffer);
        ASSERT_EQ(navi::TEC_NONE, type.serialize(ctx, data));
    }
    {
        DataBuffer readBuffer(dataBuffer.getData(), dataBuffer.getDataLen());
        navi::TypeContext ctx(readBuffer);
        DataPtr data;
        ASSERT_EQ(navi::TEC_NONE, type.deserialize(ctx, data));
        auto *typedData = dynamic_cast<SqlQueryConfigData *>(data.get());
        ASSERT_NE(nullptr, typedData);

        auto &config = typedData->getConfig();
        ASSERT_TRUE(config.resultallowsoftfailure());
    }
}

} // namespace sql
