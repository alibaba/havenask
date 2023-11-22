#include "build_service_tasks/io/MultiFileOutput.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/Span.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/io/IODefine.h"
#include "build_service_tasks/test/unittest.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::io;
using namespace build_service::config;
using namespace autil::legacy;

using namespace indexlib::file_system;

namespace build_service_tasks {

class MultiFileOutputTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void assertFile(const string& path, const string& expectContent)
    {
        string content;
        FslibWrapper::AtomicLoadE(path, content);
        ASSERT_EQ(expectContent, content);
    }

protected:
    std::string mDestDir;
};

void MultiFileOutputTest::setUp() { mDestDir = GET_TEMP_DATA_PATH(); }

void MultiFileOutputTest::tearDown() {}

TEST_F(MultiFileOutputTest, testSimple)
{
    TaskOutputConfig config;
    config.addParameters("dest_directory", mDestDir);
    config.addParameters("file_prefix", "data");
    config.addParameters("range_str", "0_1");
    MultiFileOutputPtr output(new MultiFileOutput(config));
    ASSERT_TRUE(output->init({}));
    vector<string> datas = {
        "hello",
        "world",
    };
    for (const auto& data : datas) {
        autil::StringView cs(data);
        Any any = cs;
        ASSERT_TRUE(output->write(any));
        ASSERT_TRUE(output->commit());
    }
    autil::StringView cs("END");
    Any any = cs;
    output->write(any);
    output.reset();

    assertFile(mDestDir + "/data0_1_1", datas[0]);
    assertFile(mDestDir + "/data0_1_2", datas[1]);
    assertFile(mDestDir + "/data0_1_3", string("END"));
}

TEST_F(MultiFileOutputTest, testFailover)
{
    TaskOutputConfig config;
    config.addParameters("dest_directory", mDestDir);
    config.addParameters("file_prefix", "data");
    config.addParameters("range_str", "0_1");

    auto tempFile = mDestDir + "/data0_1_1." + MultiFileOutput::TEMP_MULTI_FILE_SUFFIX;
    FslibWrapper::AtomicStoreE(tempFile, "1");
    ASSERT_TRUE(FslibWrapper::IsExist(tempFile).GetOrThrow());

    MultiFileOutputPtr output(new MultiFileOutput(config));
    ASSERT_TRUE(output->init({}));
    autil::StringView cs2("2");
    Any any = cs2;
    ASSERT_TRUE(output->write(any));
    ASSERT_TRUE(output->commit());

    ASSERT_FALSE(FslibWrapper::IsExist(tempFile).GetOrThrow());

    assertFile(mDestDir + "/data0_1_1", string("2"));
}

} // namespace build_service_tasks
