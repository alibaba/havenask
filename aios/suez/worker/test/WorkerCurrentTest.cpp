#include "suez/worker/WorkerCurrent.h"

#include "unittest/unittest.h"

namespace suez {

class WorkerCurrentTest : public TESTBASE {};

TEST_F(WorkerCurrentTest, testToHeartbeatFormat) {
    WorkerCurrent current;
    autil::legacy::json::JsonMap json;
    current.toHeartbeatFormat(json);
    ASSERT_TRUE(json.find("target_version") == json.end());
    ASSERT_TRUE(json.find("error_info") == json.end());

    current.setTargetVersion(123);
    current.toHeartbeatFormat(json);
    ASSERT_TRUE(json.find("target_version") != json.end());
    ASSERT_EQ(123, autil::legacy::AnyCast<int64_t>(json["target_version"]));
    ASSERT_TRUE(json.find("error_info") == json.end());

    current.setError(SuezError(12, "xx"));
    current.toHeartbeatFormat(json);
    ASSERT_TRUE(json.find("target_version") != json.end());
    ASSERT_TRUE(json.find("error_info") != json.end());
}

} // namespace suez
