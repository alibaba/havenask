#include "build_service/test/unittest.h"
#include "build_service/common/ProcessorTaskIdentifier.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace common {

class ProcessorTaskIdentifierTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ProcessorTaskIdentifierTest::setUp() {
}

void ProcessorTaskIdentifierTest::tearDown() {
}

TEST_F(ProcessorTaskIdentifierTest, testSimple) {
    {
        string identifierStr = "3-1-clusterName=cluster1";
        ProcessorTaskIdentifier taskId;
        ASSERT_TRUE(taskId.fromString(identifierStr));
        ASSERT_EQ(3, taskId.getDataDescriptionId());
        ASSERT_EQ("1", taskId.getTaskId());
        string clusterName;
        ASSERT_TRUE(taskId.getClusterName(clusterName));
        ASSERT_EQ("cluster1", clusterName);
        
        ASSERT_EQ(identifierStr, taskId.toString());
    }
    {
        string identifierStr = "1-clusterName=cluster1";
        ProcessorTaskIdentifier taskId;
        ASSERT_TRUE(taskId.fromString(identifierStr));
        ASSERT_EQ(1, taskId.getDataDescriptionId());
        ASSERT_EQ("0", taskId.getTaskId());
        string clusterName;
        ASSERT_TRUE(taskId.getClusterName(clusterName));
        ASSERT_EQ("cluster1", clusterName);

        ASSERT_EQ(identifierStr, taskId.toString());
    }
    {
        string identifierStr = "1";
        ProcessorTaskIdentifier taskId;
        ASSERT_TRUE(taskId.fromString(identifierStr));
        ASSERT_EQ(1, taskId.getDataDescriptionId());
        ASSERT_EQ("0", taskId.getTaskId());
        string clusterName;
        ASSERT_FALSE(taskId.getClusterName(clusterName));

        ASSERT_EQ(identifierStr, taskId.toString());
    }
    {
        ProcessorTaskIdentifier taskId;
        ASSERT_FALSE(taskId.fromString(""));
        ASSERT_FALSE(taskId.fromString("a-a"));
        ASSERT_FALSE(taskId.fromString("1-a"));
        ASSERT_FALSE(taskId.fromString("9-k1=v1-k2-k3=v3"));
        ASSERT_TRUE(taskId.fromString("9-k1=v1-k2=v2-k3=v3"));
        string value;
        ASSERT_TRUE(taskId.getValue("k1", value));
        ASSERT_EQ("v1", value);
        ASSERT_TRUE(taskId.getValue("k2", value));
        ASSERT_EQ("v2", value);
        ASSERT_TRUE(taskId.getValue("k3", value));
        ASSERT_EQ("v3", value);
        ASSERT_FALSE(taskId.getValue("k4", value));
    }
}

}
}
