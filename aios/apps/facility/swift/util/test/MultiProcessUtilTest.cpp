#include "swift/util/MultiProcessUtil.h"

#include <iostream>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "hippo/DriverCommon.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace std;
using namespace autil;
using namespace hippo;

namespace swift {
namespace util {

class MultiProcessUtilTest : public TESTBASE {

private:
    static constexpr uint32_t TIMESTAMP_STR_LENGTH = 19;
};

TEST_F(MultiProcessUtilTest, testGenerateLogSenderProcess) {
    {
        //  type is not broker or admin, nothing changed
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_ALL);
        EXPECT_EQ(info1.envs, copyInfo.envs);
    }
    {
        //  cmd is not log_sender,  nothing changed
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "xxx";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_BROKER);
        EXPECT_EQ(info1.envs, copyInfo.envs);
    }
    {
        //  type is broker, no separator
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_BROKER);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog, "${HIPPO_APP_WORKDIR}/swift_broker/logs/swift/swift.log");
        EXPECT_EQ(topicName, "topic1_broker");
    }
    {
        //  type is broker, with default separator
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1#topic2|topic3");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log#logs/swift/swift_access.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_BROKER);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog,
                  "${HIPPO_APP_WORKDIR}/swift_broker/logs/swift/swift.log#${HIPPO_APP_WORKDIR}/swift_broker/logs/swift/"
                  "swift_access.log");
        EXPECT_EQ(topicName, "topic1_broker#topic2|topic3_broker");
    }
    {
        // type is broker, with self-defined separator
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("separator", "|");
        info1.envs.emplace_back("swiftTopicPrefix", "topic1|topic2|topic3");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log|logs/swift/swift_access.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_BROKER);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog,
                  "${HIPPO_APP_WORKDIR}/swift_broker/logs/swift/swift.log|${HIPPO_APP_WORKDIR}/swift_broker/logs/swift/"
                  "swift_access.log");
        EXPECT_EQ(topicName, "topic1_broker|topic2_broker|topic3_broker");
    }
    {
        //  type is admin, no separator
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_ADMIN);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog, "${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/swift.log");
        EXPECT_EQ(topicName, "topic1_admin");
    }
    {
        //  type is admin, with default separator
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1#topic2|topic3");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log#logs/swift/swift_access.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_ADMIN);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog,
                  "${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/swift.log#${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/"
                  "swift_access.log");
        EXPECT_EQ(topicName, "topic1_admin#topic2|topic3_admin");
    }
    {
        // type is admin, with self-defined separator
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("separator", "|");
        info1.envs.emplace_back("swiftTopicPrefix", "topic1|topic2|topic3");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log|logs/swift/swift_access.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_ADMIN);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog,
                  "${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/swift.log|${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/"
                  "swift_access.log");
        EXPECT_EQ(topicName, "topic1_admin|topic2_admin|topic3_admin");
    }
    {
        // type is admin, with self-defined separator, some config is empty
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("separator", "|");
        info1.envs.emplace_back("swiftTopicPrefix", "||topic2|topic3|");
        info1.envs.emplace_back("swiftLogSuffix", "|logs/swift/swift.log||logs/swift/swift_access.log");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_ADMIN);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog,
                  "|${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/swift.log||${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/"
                  "swift_access.log");
        EXPECT_EQ(topicName, "||topic2_admin|topic3_admin|");
    }
    {
        //  test env already exists
        std::vector<ProcessInfo> processes;
        processes.emplace_back();
        ProcessInfo &info1 = *processes.rbegin();
        info1.cmd = "log_sender";
        info1.envs.emplace_back("swiftTopicPrefix", "topic1");
        info1.envs.emplace_back("swiftLogSuffix", "logs/swift/swift.log");
        info1.envs.emplace_back("topicName", "exist1");
        info1.envs.emplace_back("streamAccessLogPath", "exist2");
        ProcessInfo copyInfo(info1);
        MultiProcessUtil::generateLogSenderProcess(processes, ROLE_TYPE_ADMIN);
        EXPECT_NE(info1.envs, copyInfo.envs);
        string topicName;
        string swiftLog;
        for (auto env : info1.envs) {
            if (env.first == "topicName") {
                topicName = env.second;
            }
            if (env.first == "streamAccessLogPath") {
                swiftLog = env.second;
            }
        }
        EXPECT_EQ(swiftLog, "${HIPPO_APP_WORKDIR}/swift_admin/logs/swift/swift.log");
        EXPECT_EQ(topicName, "topic1_admin");
    }
}

} // namespace util
} // namespace swift
