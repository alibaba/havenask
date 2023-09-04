#include "sql/resource/SwiftMessageWriter.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "sql/resource/MessageWriter.h"
#include "suez/sdk/RemoteTableWriterParam.h"
#include "swift/client/helper/CheckpointBuffer.h"
#include "swift/client/helper/SwiftWriterAsyncHelper.h"
#include "swift/common/Common.h"
#include "swift/testlib/MockSwiftWriterAsyncHelper.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {
class MessageInfo;
} // namespace common
} // namespace swift

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::result;
using namespace swift::testlib;
using namespace swift::client;
using namespace swift::common;
using namespace suez;

namespace sql {

class SwiftMessageWriterTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SwiftMessageWriterTest);

void SwiftMessageWriterTest::setUp() {}

void SwiftMessageWriterTest::tearDown() {}

TEST_F(SwiftMessageWriterTest, testWriteFailed) {
    SwiftWriterAsyncHelperPtr helper;
    SwiftMessageWriter writer(helper);
    MessageWriteParam param;
    param.cb = [](Result<suez::WriteCallbackParam> result) {
        ASSERT_EQ("swift message writer is empty!", result.get_error().message());
    };
    param.timeoutUs = 1000;
    writer.write(param);
}

TEST_F(SwiftMessageWriterTest, testWriteTimeout) {
    auto mockSwiftHelper = make_shared<MockSwiftWriterAsyncHelper>();
    EXPECT_CALL(*mockSwiftHelper, write(_, _, _, _))
        .WillOnce(testing::Invoke([](MessageInfo *msgInfos,
                                     size_t count,
                                     SwiftWriteCallbackItem::WriteCallbackFunc callback,
                                     int64_t timeout) {
            timeout > 100 ? callback(SwiftWriteCallbackParam {nullptr, count})
                          : callback(RuntimeError::make("timeout"));
        }));

    SwiftMessageWriter writer(mockSwiftHelper);
    MessageWriteParam param;
    param.cb = [](Result<suez::WriteCallbackParam> result) {
        ASSERT_EQ("timeout", result.get_error().message());
    };
    param.timeoutUs = 10;
    writer.write(param);
}

TEST_F(SwiftMessageWriterTest, testWrite) {
    auto mockSwiftHelper = make_shared<MockSwiftWriterAsyncHelper>();
    EXPECT_CALL(*mockSwiftHelper, write(_, _, _, _))
        .WillOnce(testing::Invoke([](MessageInfo *msgInfos,
                                     size_t count,
                                     SwiftWriteCallbackItem::WriteCallbackFunc callback,
                                     int64_t timeout) {
            timeout > 100 ? callback(SwiftWriteCallbackParam {nullptr, count})
                          : callback(RuntimeError::make("timeout"));
        }));

    SwiftMessageWriter writer(mockSwiftHelper);
    MessageWriteParam param;
    param.cb = [](Result<suez::WriteCallbackParam> result) { ASSERT_FALSE(result.is_err()); };
    param.timeoutUs = 1000;
    writer.write(param);
}

} // namespace sql
