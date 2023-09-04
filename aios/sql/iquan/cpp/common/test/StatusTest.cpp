#include "iquan/common/Status.h"

#include <string>

#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/Common.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class StatusTest : public TESTBASE {};

TEST_F(StatusTest, testConstructor1) {
    Status status;
    ASSERT_EQ(IQUAN_OK, status.code());
    ASSERT_EQ("", status.errorMessage());
}

TEST_F(StatusTest, testConstructor2) {
    Status status(-1, "test status");
    ASSERT_EQ(-1, status.code());
    ASSERT_EQ("test status", status.errorMessage());
}

TEST_F(StatusTest, testCopyConstructor) {
    Status status(-1, "test status");
    ASSERT_EQ(-1, status.code());
    ASSERT_EQ("test status", status.errorMessage());

    Status anotherStatus = status;
    ASSERT_EQ(-1, anotherStatus.code());
    ASSERT_EQ("test status", anotherStatus.errorMessage());

    Status anotherStatus2(status);
    ASSERT_EQ(-1, anotherStatus2.code());
    ASSERT_EQ("test status", anotherStatus2.errorMessage());
}

TEST_F(StatusTest, testEqualAndNotEqual) {
    Status status(-1, "test status");
    ASSERT_EQ(-1, status.code());
    ASSERT_EQ("test status", status.errorMessage());

    Status anotherStatus = status;
    ASSERT_EQ(-1, anotherStatus.code());
    ASSERT_EQ("test status", anotherStatus.errorMessage());

    ASSERT_TRUE(status == anotherStatus);
    ASSERT_FALSE(status != anotherStatus);

    Status status2(-2, "test status");
    ASSERT_EQ(-2, status2.code());
    ASSERT_EQ("test status", status.errorMessage());
    ASSERT_FALSE(status == status2);
    ASSERT_TRUE(status != status2);

    Status status3(-1, "test status in status3");
    ASSERT_EQ(-1, status3.code());
    ASSERT_EQ("test status in status3", status3.errorMessage());
    ASSERT_FALSE(status == status3);
    ASSERT_TRUE(status != status3);
}

TEST_F(StatusTest, testStatusJsonWrapper) {
    Status status(-1, "test status");
    StatusJsonWrapper statusJson;
    statusJson.status = status;
    std::string jsonStr = autil::legacy::FastToJsonString(statusJson, true);
    std::string expectStr = "{\"error_code\":-1,\"error_message\":\"test status\"}";
    ASSERT_NO_FATAL_FAILURE(autil::legacy::JsonTestUtil::checkJsonStringEqual(expectStr, jsonStr));
}

} // namespace iquan
