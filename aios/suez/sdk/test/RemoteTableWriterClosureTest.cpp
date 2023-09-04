#include "suez/sdk/RemoteTableWriterClosure.h"

#include "autil/Lock.h"
#include "multi_call/interface/Reply.h"
#include "suez/sdk/RemoteTableWriterRequestGenerator.h"
#include "suez/service/Service.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace suez {

class MockReply : public multi_call::Reply {
public:
    MOCK_METHOD2(getResponses,
                 std::vector<multi_call::ResponsePtr>(size_t &lackCount,
                                                      std::vector<multi_call::LackResponseInfo> &lackInfos));
};

class RemoteTableWriterClosureTest : public TESTBASE {};

TEST_F(RemoteTableWriterClosureTest, testRun) {
    autil::Notifier notifier;
    auto cb = [&](autil::result::Result<WriteCallbackParam> result) { notifier.notifyExit(); };
    RemoteTableWriterClosure *closure = new RemoteTableWriterClosure(std::move(cb));
    closure->Run();
    notifier.waitNotification();
}

TEST_F(RemoteTableWriterClosureTest, testDoRun_Success) {
    auto remoteResp = make_shared<RemoteTableWriterResponse>(nullptr);
    remoteResp->_stat.ec = multi_call::MULTI_CALL_ERROR_NONE;
    auto *response = new WriteResponse();
    response->set_checkpoint(123);
    response->set_buildgap(1);
    remoteResp->_message = response;
    vector<multi_call::ResponsePtr> responseVec = {remoteResp};
    auto mockReply = make_shared<MockReply>();
    EXPECT_CALL(*mockReply, getResponses(_, _)).WillOnce(DoAll(SetArgReferee<0>(0), Return(responseVec)));
    autil::Notifier notifier;
    auto cb = [&](autil::result::Result<WriteCallbackParam> result) {
        [&] {
            ASSERT_TRUE(result.is_ok()) << result.get_error().message();
            WriteCallbackParam param = result.get();
            ASSERT_EQ(123, param.maxCp);
            ASSERT_EQ(1, param.maxBuildGap);
        }();
        notifier.notifyExit();
    };
    RemoteTableWriterClosure closure(std::move(cb));
    closure._reply = mockReply;
    closure.doRun();
    notifier.waitNotification();
}

TEST_F(RemoteTableWriterClosureTest, testGetDataFromResponse_Success) {
    auto remoteResp = make_shared<RemoteTableWriterResponse>(nullptr);
    remoteResp->_stat.ec = multi_call::MULTI_CALL_ERROR_DEC_WEIGHT;
    remoteResp->_message = new WriteResponse();
    auto cb = [&](autil::result::Result<WriteCallbackParam> result) { EXPECT_FALSE(true); };
    RemoteTableWriterClosure closure(std::move(cb));
    ASSERT_TRUE(closure.getDataFromResponse(remoteResp));
}

} // namespace suez
