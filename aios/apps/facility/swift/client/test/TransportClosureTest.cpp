#include "swift/client/TransportClosure.h"

#include "autil/CommonMacros.h"
#include "swift/client/Notifier.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace swift::protocol;

namespace swift {
namespace client {

class TransportClosureTest : public TESTBASE {};

TEST_F(TransportClosureTest, testSimpleProcess) {
    ConsumptionRequest *request = new ConsumptionRequest;
    Notifier notifier;
    notifier.setNeedNotify(true);
    TransportClosureTyped<TRT_GETMESSAGE> closure(request, &notifier);
    EXPECT_TRUE(!closure.isDone());
    EXPECT_TRUE(!notifier.wait(1000));
    closure.Run();
    EXPECT_TRUE(notifier.wait(-1));
    EXPECT_TRUE(closure.isDone());
    EXPECT_TRUE(closure.getRequest());
    EXPECT_TRUE(closure.getResponse());
    MessageResponse *response;
    response = closure.stealResponse();
    EXPECT_TRUE(!closure.getResponse());
    DELETE_AND_SET_NULL(response);
}

} // namespace client
} // namespace swift
