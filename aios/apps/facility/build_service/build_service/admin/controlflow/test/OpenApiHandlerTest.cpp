#include "build_service/admin/controlflow/OpenApiHandler.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>

#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace admin {

class OpenApiHandlerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
};

void OpenApiHandlerTest::setUp() {}

void OpenApiHandlerTest::tearDown() {}

TEST_F(OpenApiHandlerTest, testUpdateIndexCheckpointResource)
{
    string configRootPath = GET_TEST_DATA_PATH() + "build_flow_test/config_src_node";
    config::ResourceReaderPtr configReader(new config::ResourceReader(configRootPath));
    configReader->init();
    config::ConfigReaderAccessorPtr accessor(new config::ConfigReaderAccessor("simple"));
    accessor->addConfig(configReader, true);
    TaskResourceManagerPtr taskResourceManager(new TaskResourceManager);
    ASSERT_TRUE(taskResourceManager->addResource(accessor));
    common::CheckpointAccessorPtr checkpointAccessor(new common::CheckpointAccessor());
    ASSERT_TRUE(taskResourceManager->addResource(checkpointAccessor));
    OpenApiHandler handler(taskResourceManager);
    {
        KeyValueMap bookParam;
        bookParam["clusterName"] = "cluster1";
        bookParam["version"] = "latest";
        bookParam["name"] = "simple_name";
        ASSERT_TRUE(handler.handle("book", "/resource/index_checkpoint", bookParam));
    }
    {
        KeyValueMap updateParam;
        updateParam["name"] = "simple_name";
        updateParam["visiable"] = "111222333";
        ASSERT_FALSE(handler.handle("update", "/resource/index_checkpoint", updateParam));
        updateParam["visiable"] = "true";
        ASSERT_TRUE(handler.handle("update", "/resource/index_checkpoint", updateParam));
        updateParam["visiable"] = "false";
        ASSERT_TRUE(handler.handle("update", "/resource/index_checkpoint", updateParam));
    }
}

}} // namespace build_service::admin
