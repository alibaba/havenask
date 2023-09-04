#include "swift/admin/ParaChecker.h"

#include <iosfwd>
#include <string>

#include "unittest/unittest.h"

namespace swift {
namespace admin {

using namespace std;

class ParaCheckerTest : public TESTBASE {
protected:
    void checkValidIpAddressTest();
    void checkValidTopicNameTest();
};

void ParaCheckerTest::checkValidTopicNameTest() {
    ParaChecker checker;
    std::string topicName;
    bool ret;

    topicName = "abz.3847_ajk-v3";
    ret = checker.checkValidTopicName(topicName);
    EXPECT_TRUE(ret);

    topicName = "abz_ajk___";
    ret = checker.checkValidTopicName(topicName);
    EXPECT_TRUE(ret);

    topicName = "_asdhf238";
    ret = checker.checkValidTopicName(topicName);
    EXPECT_EQ(ret, false);

    topicName = "asd3847_akdj#$%";
    ret = checker.checkValidTopicName(topicName);
    EXPECT_EQ(ret, false);

    topicName = "8.a847843)*(&";
    ret = checker.checkValidTopicName(topicName);
    EXPECT_EQ(ret, false);

    topicName = ".a847aksd";
    ret = checker.checkValidTopicName(topicName);
    EXPECT_EQ(ret, false);
}

void ParaCheckerTest::checkValidIpAddressTest() {
    ParaChecker checker;
    std::string ipAddress;
    bool ret;

    ipAddress = "10.250.8.93";
    ret = checker.checkValidIpAddress(ipAddress);
    EXPECT_TRUE(ret);

    ipAddress = "010.250.008.093";
    ret = checker.checkValidIpAddress(ipAddress);
    EXPECT_TRUE(ret);

    ipAddress = "010.0250.008.093";
    ret = checker.checkValidIpAddress(ipAddress);
    EXPECT_EQ(ret, false);

    ipAddress = "10.250.9.93.5";
    ret = checker.checkValidIpAddress(ipAddress);
    EXPECT_EQ(ret, false);
}

} // namespace admin
} // namespace swift
