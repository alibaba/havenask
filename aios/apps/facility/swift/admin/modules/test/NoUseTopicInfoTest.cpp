#include "swift/admin/modules/NoUseTopicInfo.h"

#include <iosfwd>
#include <map>
#include <string>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib::fs;
using namespace fslib;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace admin {

class NoUseTopicInfoTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void NoUseTopicInfoTest::setUp() {}

void NoUseTopicInfoTest::tearDown() {}

TEST_F(NoUseTopicInfoTest, testGetLastNoUseTopicsMeta) {
    NoUseTopicInfo info;
    TopicCreationRequest meta;
    meta.set_topicname("topic1");
    meta.set_partitioncount(1);
    info._lastNoUseTopics["topic1"] = meta;
    meta.set_topicname("topic2");
    meta.set_partitioncount(2);
    info._lastNoUseTopics["topic2"] = meta;
    LastDeletedNoUseTopicResponse response;
    info.getLastNoUseTopicsMeta(&response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    ASSERT_EQ(2, response.topicmetas_size());
    auto resme = response.topicmetas(0);
    ASSERT_EQ("topic1", resme.topicname());
    ASSERT_EQ(1, resme.partitioncount());
    resme = response.topicmetas(1);
    ASSERT_EQ("topic2", resme.topicname());
    ASSERT_EQ(2, resme.partitioncount());
}

}; // namespace admin
}; // namespace swift
