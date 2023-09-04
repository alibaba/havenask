

#include "swift/admin/AdminRequestChecker.h"

#include <string>

#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace admin {

class AdminRequestCheckerTest : public TESTBASE {};

using namespace swift::protocol;
using namespace autil::result;

TEST_F(AdminRequestCheckerTest, checkTopicCreationRequestTest) {
    TopicCreationRequest request;
    Result<bool> ret;
    config::AdminConfig config;

    config.dfsRoot = "";
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_topicname("cnzz");
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_partitioncount(8);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_partitionminbuffersize(0);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_partitionminbuffersize(256 * 1024 + 1);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_partitionminbuffersize(1);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_partitionmaxbuffersize(0);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_TRUE(!ret.is_ok());

    request.set_partitionmaxbuffersize(1);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_TRUE(ret.is_ok());

    request.set_partitionmaxbuffersize(262145);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_TRUE(!ret.is_ok());

    request.set_partitionmaxbuffersize(256 * 1024);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_TRUE(ret.is_ok());

    request.set_partitionminbuffersize(256 * 1024);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_partitioncount(100);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_resource(0);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_resource(400);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_resource(10001);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_resource(10000);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_partitionlimit(0);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_partitionlimit(20);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_obsoletefiletimeinterval(0);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_obsoletefiletimeinterval(-2);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_obsoletefiletimeinterval(-1);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_obsoletefiletimeinterval(2);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_reservedfilecount(0);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_reservedfilecount(-2);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_reservedfilecount(1);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_reservedfilecount(-1);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_rangecountinpartition(4);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);
    request.set_rangecountinpartition(1);

    request.set_reservedfilecount(2);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_topicmode(TOPIC_MODE_SECURITY);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    config.dfsRoot = "/abc";
    request.set_topicmode(TOPIC_MODE_SECURITY);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_needfieldfilter(true);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_needschema(true);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_needfieldfilter(false);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_topictype(TOPIC_TYPE_PHYSIC);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_topicname("test-123-4");
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);

    request.set_topicname("test-123-0");
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_topicname("test-123-ab");
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_topicname("test");
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), false);

    request.set_topicname("cnzz");
    request.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    ret = AdminRequestChecker::checkTopicCreationRequest(&request, &config);
    ASSERT_EQ(ret.is_ok(), true);
}

TEST_F(AdminRequestCheckerTest, checkTopicDeletionRequestTest) {
    AdminRequestChecker checker;
    TopicDeletionRequest request;
    bool ret;

    ret = AdminRequestChecker::checkTopicDeletionRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_topicname("cnzz");
    ret = AdminRequestChecker::checkTopicDeletionRequest(&request);
    ASSERT_TRUE(ret);
}

TEST_F(AdminRequestCheckerTest, checkTopicInfoRequestTest) {
    AdminRequestChecker checker;
    TopicInfoRequest request;
    bool ret;

    ret = AdminRequestChecker::checkTopicInfoRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_topicname("cnzz");
    ret = AdminRequestChecker::checkTopicInfoRequest(&request);
    ASSERT_TRUE(ret);
}

TEST_F(AdminRequestCheckerTest, checkPartitionInfoRequestTest) {
    AdminRequestChecker checker;
    PartitionInfoRequest request;
    bool ret;

    ret = AdminRequestChecker::checkPartitionInfoRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_topicname("cnzz");
    ret = AdminRequestChecker::checkPartitionInfoRequest(&request);
    ASSERT_EQ(ret, false);

    request.add_partitionids(0);
    ret = AdminRequestChecker::checkPartitionInfoRequest(&request);
    ASSERT_TRUE(ret);
}

TEST_F(AdminRequestCheckerTest, checkRoleAddressRequestTest) {
    AdminRequestChecker checker;
    RoleAddressRequest request;
    bool ret;

    request.set_role(ROLE_TYPE_NONE);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_role(ROLE_TYPE_ALL);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_role(ROLE_TYPE_ADMIN);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_role(ROLE_TYPE_BROKER);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_status(ROLE_STATUS_NONE);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_EQ(ret, false);

    request.set_status(ROLE_STATUS_ALL);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_TRUE(ret);

    request.set_status(ROLE_STATUS_LIVING);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_TRUE(ret);

    request.set_status(ROLE_STATUS_DEAD);
    ret = AdminRequestChecker::checkRoleAddressRequest(&request);
    ASSERT_TRUE(ret);
}

TEST_F(AdminRequestCheckerTest, checkErrorRequestTest) {
    ErrorRequest request1, request2;
    bool ret;

    ret = AdminRequestChecker::checkErrorRequest(&request1);
    ASSERT_EQ(ret, false);

    request1.set_time(134143143);
    ret = AdminRequestChecker::checkErrorRequest(&request1);
    ASSERT_TRUE(ret);

    request1.set_count(49032);
    ret = AdminRequestChecker::checkErrorRequest(&request1);
    ASSERT_TRUE(ret);

    request1.set_level(ERROR_LEVEL_FATAL);
    ret = AdminRequestChecker::checkErrorRequest(&request1);
    ASSERT_TRUE(ret);

    ret = AdminRequestChecker::checkErrorRequest(&request2);
    ASSERT_EQ(ret, false);

    request2.set_count(2349);
    ret = AdminRequestChecker::checkErrorRequest(&request2);
    ASSERT_TRUE(ret);

    request2.set_time(9889);
    ret = AdminRequestChecker::checkErrorRequest(&request2);
    ASSERT_TRUE(ret);
}

TEST_F(AdminRequestCheckerTest, checkLogicTopicModifyTest) {
    TopicCreationRequest target;
    TopicCreationRequest current;
    current.set_topicname("test");
    current.set_partitioncount(1);
    target.set_topicname("test");
    target.set_partitioncount(1);

    // L -> L
    current.set_topictype(TOPIC_TYPE_LOGIC);
    target.set_topictype(TOPIC_TYPE_LOGIC);
    target.set_partitionlimit(10);
    ASSERT_TRUE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // LP -> LP
    current.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    target.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    target.set_partitioncount(2);
    ASSERT_TRUE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // N -> LP
    current.set_topictype(TOPIC_TYPE_NORMAL);
    target.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));
    target.set_partitioncount(1);
    ASSERT_TRUE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // N -> LP
    current.set_topictype(TOPIC_TYPE_NORMAL);
    current.set_sealed(true);
    target.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    target.set_sealed(false);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));
    current.set_sealed(false);

    // N -> LP
    current.set_topictype(TOPIC_TYPE_NORMAL);
    *current.add_extenddfsroot() = "exdfsroot";
    target.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    *target.add_extenddfsroot() = "exdfsroot";
    ASSERT_TRUE(AdminRequestChecker::checkLogicTopicModify(target, current));
    *target.add_extenddfsroot() = "exdfsroot2";
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // L -> N
    current.clear_extenddfsroot();
    target.clear_extenddfsroot();
    target.set_partitionlimit(100000);
    current.set_topictype(TOPIC_TYPE_LOGIC);
    target.set_topictype(TOPIC_TYPE_NORMAL);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // L -> LP
    current.set_topictype(TOPIC_TYPE_LOGIC);
    target.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // LP -> N
    current.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    target.set_topictype(TOPIC_TYPE_NORMAL);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // LP -> L
    current.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    target.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));

    // N -> L
    current.set_topictype(TOPIC_TYPE_NORMAL);
    target.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_FALSE(AdminRequestChecker::checkLogicTopicModify(target, current));
}

} // namespace admin
} // namespace swift
