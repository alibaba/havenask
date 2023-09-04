#include "sql/data/SqlGraphType.h"

#include <engine/TypeContext.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/util/NaviTestPool.h"
#include "sql/data/SqlGraphData.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace navi;
using namespace autil;

namespace sql {

class SqlGraphTypeTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(data, SqlGraphTypeTest);

void SqlGraphTypeTest::setUp() {}

void SqlGraphTypeTest::tearDown() {}

TEST_F(SqlGraphTypeTest, testSimple) {
    std::unique_ptr<GraphDef> graphDef(new GraphDef());
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph("biz_1");
    builder.node("node1_1").kernel("TestKernel").out("out1").asGraphOutput("o1");
    vector<string> ports = {"output1", "output2"};
    vector<string> nodes = {"1", "1"};
    SqlGraphDataPtr sqlGraphData(new SqlGraphData(ports, nodes, graphDef));
    autil::mem_pool::PoolPtr pool(new autil::mem_pool::PoolAsan);

    DataBuffer dataBuffer(2048, pool.get());
    TypeContext ctx(dataBuffer);
    SqlGraphType type;
    ASSERT_EQ(navi::TEC_NONE, type.serialize(ctx, sqlGraphData));

    DataPtr data;
    ASSERT_EQ(navi::TEC_NONE, type.deserialize(ctx, data));
    auto sqlGraphData2 = dynamic_pointer_cast<SqlGraphData>(data);
    ASSERT_TRUE(sqlGraphData2 != nullptr);
    ASSERT_TRUE(sqlGraphData2->getGraphDef() != nullptr);
    ASSERT_TRUE(sqlGraphData2->getGraphDef()->ShortDebugString()
                == sqlGraphData->getGraphDef()->ShortDebugString());
    ASSERT_EQ(sqlGraphData2->getOutputPorts(), sqlGraphData->getOutputPorts());
    ASSERT_EQ(sqlGraphData2->getOutputNodes(), sqlGraphData->getOutputNodes());
}

} // namespace sql
