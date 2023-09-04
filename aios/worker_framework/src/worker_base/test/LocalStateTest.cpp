#include "worker_framework/LocalState.h"

#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

namespace worker_framework {

class LocalStateTest : public TESTBASE {
public:
    void setUp() override { _testDir = GET_TEMP_DATA_PATH(); }

protected:
    std::string _testDir;
};

TEST_F(LocalStateTest, testReadWrite) {
    auto stateFile = fslib::fs::FileSystem::joinFilePath(_testDir, "state");
    auto state = std::make_unique<LocalState>(std::move(stateFile));

    std::string content;
    auto ec = state->read(content);
    ASSERT_EQ(WorkerState::EC_NOT_EXIST, ec);

    ec = state->write("abcd");
    ASSERT_EQ(WorkerState::EC_OK, ec);

    ec = state->read(content);
    ASSERT_EQ(WorkerState::EC_OK, ec);
    ASSERT_EQ("abcd", content);
}

} // namespace worker_framework
