#include "build_service/test/unittest.h"
#include "build_service/io/MultiFileOutput.h"
#include <autil/StringUtil.h>
#include <autil/ConstString.h>
#include <autil/legacy/jsonizable.h>
#include <indexlib/storage/file_system_wrapper.h>

using namespace std;
using namespace testing;
using namespace build_service::io;
using namespace autil::legacy;

IE_NAMESPACE_USE(storage);

namespace build_service {
namespace io {

class MultiFileOutputTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    void assertFile(const string& path, const string& expectContent) {
        string content;
        FileSystemWrapper::AtomicLoad(path, content);
        ASSERT_EQ(expectContent, content);
    }

protected:
    std::string mDestDir;
};

void MultiFileOutputTest::setUp() {
    mDestDir = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(), "test");
    FileSystemWrapper::MkDir(mDestDir);
}

void MultiFileOutputTest::tearDown() {}

TEST_F(MultiFileOutputTest, testSimple) {
    TaskOutputConfig config;
    config.addParameters("dest_directory", mDestDir);
    config.addParameters("file_prefix", "data");
    MultiFileOutputPtr output(new MultiFileOutput(config));
    ASSERT_TRUE(output->init({}));
    vector<string> datas = {
        "hello",
        "world",
    };
    for (const auto& data : datas) {
        autil::ConstString cs(data);
        Any any = cs;
        ASSERT_TRUE(output->write(any));
        ASSERT_TRUE(output->commit());
    }
    autil::ConstString cs("END");
    Any any = cs;
    output->write(any);
    output.reset();

    assertFile(mDestDir + "/data_1", datas[0]);
    assertFile(mDestDir + "/data_2", datas[1]);
    assertFile(mDestDir + "/data_3", string("END"));
}

TEST_F(MultiFileOutputTest, testFailover) {
    TaskOutputConfig config;
    config.addParameters("dest_directory", mDestDir);
    config.addParameters("file_prefix", "data");

    auto tempFile = mDestDir + "/data_1." + MultiFileOutput::TEMP_MULTI_FILE_SUFFIX;
    FileSystemWrapper::AtomicStore(tempFile, "1");
    ASSERT_TRUE(FileSystemWrapper::IsExist(tempFile));

    MultiFileOutputPtr output(new MultiFileOutput(config));
    ASSERT_TRUE(output->init({}));
    autil::ConstString cs2("2");
    Any any = cs2;
    ASSERT_TRUE(output->write(any));
    ASSERT_TRUE(output->commit());

    ASSERT_FALSE(FileSystemWrapper::IsExist(tempFile));

    assertFile(mDestDir + "/data_1", string("2"));
}

}  // namespace io
}  // namespace build_service
