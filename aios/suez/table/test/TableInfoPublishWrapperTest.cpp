#include "suez/table/TableInfoPublishWrapper.h"

#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace multi_call;

namespace suez {

class TableInfoPublishWrapperTest : public TESTBASE {
public:
    multi_call::GigRpcServerPtr createGigRpcServer() {
        multi_call::GigRpcServerPtr gigServer(new multi_call::GigRpcServer());
        multi_call::GrpcServerDescription desc;
        auto grpcPortStr = autil::StringUtil::toString(0);
        desc.port = grpcPortStr;
        desc.ioThreadNum = 1;
        if (!gigServer->initGrpcServer(desc)) {
            return nullptr;
        }
        return gigServer;
    }
};

TEST_F(TableInfoPublishWrapperTest, testSimple) {
    auto gigRpcServer = createGigRpcServer();
    auto publishWrapper = std::make_shared<TableInfoPublishWrapper>(gigRpcServer.get());
    ServerBizTopoInfo info;
    info.bizName = "default";
    info.partCount = 1;
    info.partId = 0;
    info.version = 0;
    SignatureTy signature = 0;
    ASSERT_TRUE(publishWrapper->publish(info, signature));
    ASSERT_NE(0, signature);
    ASSERT_EQ(1, publishWrapper->_counterMap[signature]);
    ASSERT_EQ(1, gigRpcServer->getPublishInfos().size());

    ASSERT_TRUE(publishWrapper->publish(info, signature));
    ASSERT_EQ(2, publishWrapper->_counterMap[signature]);
    ASSERT_EQ(1, gigRpcServer->getPublishInfos().size());

    ASSERT_TRUE(publishWrapper->unpublish(signature));
    ASSERT_EQ(1, publishWrapper->_counterMap[signature]);
    ASSERT_EQ(1, gigRpcServer->getPublishInfos().size());

    ASSERT_TRUE(publishWrapper->unpublish(signature));
    ASSERT_TRUE(publishWrapper->_counterMap.empty());
    ASSERT_EQ(0, gigRpcServer->getPublishInfos().size());
}

} // namespace suez
