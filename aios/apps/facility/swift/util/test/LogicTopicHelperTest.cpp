#include "swift/util/LogicTopicHelper.h"

#include <iosfwd>
#include <limits>
#include <stdint.h>
#include <string>

#include "unittest/unittest.h"

namespace swift {
namespace util {

using namespace std;

class LogicTopicHelperTest : public TESTBASE {};

TEST_F(LogicTopicHelperTest, testGetLogicTopicName) {
    string logicName;
    ASSERT_TRUE(LogicTopicHelper::getLogicTopicName("topic_123-23423-3", logicName));
    ASSERT_EQ("topic_123", logicName);
    ASSERT_FALSE(LogicTopicHelper::getLogicTopicName("topic-1233", logicName));
    ASSERT_FALSE(LogicTopicHelper::getLogicTopicName("-23423-3", logicName));
    ASSERT_FALSE(LogicTopicHelper::getLogicTopicName("--3", logicName));
    ASSERT_FALSE(LogicTopicHelper::getLogicTopicName("--", logicName));
    ASSERT_FALSE(LogicTopicHelper::getLogicTopicName("-", logicName));
    ASSERT_FALSE(LogicTopicHelper::getLogicTopicName("", logicName));
}

TEST_F(LogicTopicHelperTest, testGenPhysicTopicName) {
    ASSERT_EQ("topic-123-456", LogicTopicHelper::genPhysicTopicName("topic", 123, 456));
    ASSERT_EQ("topic---0-1", LogicTopicHelper::genPhysicTopicName("topic--", 0, 1));
}

TEST_F(LogicTopicHelperTest, testParsePhysicTopicName) {
    string logic;
    int64_t ts = 0;
    uint32_t pc = 0;
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("-", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("--", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("---", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("aaa-wfs-a", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("aaa-wfs-2", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("-123-2", logic, ts, pc));
    ASSERT_FALSE(LogicTopicHelper::parsePhysicTopicName("aaa-123-0", logic, ts, pc));
    ASSERT_TRUE(LogicTopicHelper::parsePhysicTopicName("aaa-123-2", logic, ts, pc));
    ASSERT_EQ("aaa", logic);
    ASSERT_EQ(123, ts);
    ASSERT_EQ(2, pc);
}

TEST_F(LogicTopicHelperTest, testGetPhysicTopicExpiredTime) {
    ASSERT_EQ(std::numeric_limits<int64_t>::max(),
              LogicTopicHelper::getPhysicTopicExpiredTime(0, -1, std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(-1, LogicTopicHelper::getPhysicTopicExpiredTime(123000000, -1, std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(-1, LogicTopicHelper::getPhysicTopicExpiredTime(123000000, 0, std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(123 + 2 * 3600,
              LogicTopicHelper::getPhysicTopicExpiredTime(123000000, 2, std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(123 + 123, LogicTopicHelper::getPhysicTopicExpiredTime(123000000, -1, 123));
    ASSERT_EQ(-1, LogicTopicHelper::getPhysicTopicExpiredTime(123000000, -1, -200));
}

} // namespace util
} // namespace swift
