#include "build_service/util/KmonitorUtil.h"

#include <iostream>
#include <string>

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "unittest/unittest.h"

namespace build_service { namespace util {

class KmonitorUtilTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(KmonitorUtilTest, testGetTags)
{
    proto::PartitionId pid;
    *pid.add_clusternames() = "cluster1";
    pid.set_role(proto::ROLE_TASK);
    std::string taskId = "taskId=fullBuilder-name=master-taskName=builderV2";
    pid.set_taskid(taskId);

    kmonitor::MetricsTags tags;
    KmonitorUtil::getTags(pid, tags);
    std::cout << "get tags [" << tags.ToString() << "]" << std::endl;
    ASSERT_EQ("builder", tags.FindTag("role"));
    ASSERT_EQ("full", tags.FindTag("step"));
    ASSERT_EQ("cluster1", tags.FindTag("cluster"));
}

}} // namespace build_service::util
