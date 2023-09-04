#include "swift/heartbeat/ZkHeartbeatWrapper.h"

#include <iosfwd>
#include <string>

#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;
namespace swift {
namespace heartbeat {

class ZkHeartbeatWrapperTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(ZkHeartbeatWrapperTest, testGetHeartBeatInfoChange) {
    HeartbeatInfo heartbeat;
    heartbeat.set_sessionid(123456789);
    heartbeat.set_alive(false);
    heartbeat.set_address("broker_1##10.1.1:100");
    heartbeat.set_sessionid(1617854155394204);
    TaskStatus *task = heartbeat.add_tasks();
    TaskInfo *tinfo = task->mutable_taskinfo();
    PartitionId *partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic");
    partId->set_id(100);
    partId->set_topicgroup("default");
    task->set_status(PARTITION_STATUS_STOPPING);

    string value;
    heartbeat.SerializeToString(&value);
    HeartbeatInfo tmp;
    ASSERT_TRUE(tmp.ParseFromString(value));
    ASSERT_EQ(heartbeat, tmp);
    for (int i = 1; i < value.size(); ++i) {
        string trunced(value.c_str(), value.size() - i);
        if (tmp.ParseFromString(trunced)) {
            // cout << "origin str: " << value << endl;
            // cout << "trunc  str: " << trunced << endl;
            // cout << "origin proto: " << heartbeat.ShortDebugString() << endl;
            // cout << "trunc  proto: " << tmp.ShortDebugString() << endl;
            ASSERT_FALSE(heartbeat == tmp);
            break;
        }
    }
}

} // namespace heartbeat
} // namespace swift
