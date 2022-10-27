#include "build_service/test/unittest.h"
#include "build_service/workflow/test/MockRealtimeBuilder.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace testing;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::proto;

namespace build_service {
namespace workflow {

class RealtimeBuilderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
public:
    RealtimeBuilderTest()
    : _builderResource(RealtimeBuilderResource(kmonitor::MetricsReporterPtr(),
                                               IE_NAMESPACE(util)::TaskSchedulerPtr(),
                                               SwiftClientCreatorPtr(),
                                               BuildFlowThreadResource()))
        {}
protected:
    string _configPath;
    RealtimeBuilderResource _builderResource;
};

void RealtimeBuilderTest::setUp() {
    _configPath = TEST_DATA_PATH"/realtime_builder_test";
}

void RealtimeBuilderTest::tearDown() {
}

TEST_F(RealtimeBuilderTest, testGetSwiftInfo) {
    MockRealtimeBuilder rtBuilder(_configPath, IndexPartitionPtr(),
                                  _builderResource);
    EXPECT_CALL(rtBuilder, getIndexRoot())
        .WillOnce(Return(TEST_DATA_PATH"realtime_builder_test/runtimedata/cluster_1/generation_0/partition_0_65535"));
    KeyValueMap kvMap;
    EXPECT_TRUE(rtBuilder.getRealtimeInfo(kvMap));
    EXPECT_EQ(kvMap[PROCESSED_DOC_SWIFT_ROOT], "swift_root");
    EXPECT_EQ(kvMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX], "user_service");
}

TEST_F(RealtimeBuilderTest, testGetRealtimeInfo) {
    MockRealtimeBuilder rtBuilder(_configPath, IndexPartitionPtr(),
                                  _builderResource);
    EXPECT_CALL(rtBuilder, getIndexRoot())
        .WillOnce(Return(TEST_DATA_PATH"realtime_builder_test/runtimedata/cluster_2/generation_0/partition_0_65535"));
    EXPECT_CALL(rtBuilder, downloadConfig(_,_))
        .WillOnce(Return(true)); 
    KeyValueMap kvMap;
    EXPECT_TRUE(rtBuilder.getRealtimeInfo(kvMap));
    EXPECT_EQ(REALTIME_JOB_MODE, kvMap[REALTIME_MODE]);
} 

}
}
