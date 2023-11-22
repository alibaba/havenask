#include "swift/broker/storage_dfs/MessageCommitter.h"

#include <functional>
#include <gmock/gmock-matchers.h>
#include <iosfwd>
#include <memory>
#include <stdlib.h>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/DllWrapper.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemFactory.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/broker/storage_dfs/test/MockFileManager.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/config/PartitionConfig.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace swift::protocol;
using namespace swift::common;

using ::testing::Ge;
using ::testing::Le;

namespace swift {
namespace storage {
#define FSLIB_MOCK_FORWARD_FUNCTION_NAME "mockForward"
class MessageCommitterTest : public TESTBASE {
public:
    typedef std::function<fslib::ErrorCode(const std::string &path, const std::string &args, std::string &output)>
        ForwardFunc;
    typedef void (*MockForwardFunc)(const std::string &, const ForwardFunc &);

public:
    void makeMessages(Messages &msgs, int count) {
        for (int i = 0; i < count; ++i) {
            Message *msg = msgs.add_msgs();
            msg->set_msgid(i);
            msg->set_timestamp(i);
            *msg->mutable_data() = std::to_string(i);
            msg->set_uint16payload(i);
            msg->set_uint8maskpayload(i);
            msg->set_compress(false);
            msg->set_merged(false);
        }
    }
};

TEST_F(MessageCommitterTest, testWriteProtoMessages) {
    PartitionId pid;
    config::PartitionConfig config;
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    FileManager fileManager;
    ASSERT_EQ(ERROR_NONE, fileManager.init(config.getDataRoot(), ObsoleteFileCriterion()));
    ASSERT_EQ(0, fileManager.getUsedDfsSize());
    MessageCommitter committer(pid, config, &fileManager, nullptr);

    Messages msgs;
    makeMessages(msgs, 10);
    ASSERT_EQ(ERROR_NONE, committer.write(msgs.msgs()));
    ASSERT_EQ(ERROR_NONE, committer.commitFile());
    ASSERT_EQ(ERROR_NONE, committer.closeFile());
    ASSERT_EQ(9, committer.getWritedId());
    ASSERT_EQ(9, committer.getCommittedId());
    ASSERT_EQ(10 + 10 * FILE_MESSAGE_META_SIZE, fileManager.getUsedDfsSize());

    auto filePair = fileManager.getFilePairById(0);
    ASSERT_TRUE(filePair);
    ASSERT_EQ(10, filePair->getMessageCount());
    ASSERT_FALSE(filePair->getNext());
    ASSERT_EQ(10, filePair->getEndMessageId());
}

TEST_F(MessageCommitterTest, testSetSealError) {
    PartitionId pid;
    config::PartitionConfig config;
    MockFileManager fileManager;
    MessageCommitter committer(pid, config, &fileManager, nullptr);

    // 1. call refreshVersion and return ERROR_BROKER_INLINE_VERSION_INVALID
    EXPECT_TRUE(committer._canRefeshVersion);
    EXPECT_CALL(fileManager, refreshVersion()).Times(1).WillOnce(Return(ERROR_BROKER_INLINE_VERSION_INVALID));
    committer.setSealError(fslib::EC_PANGU_USER_DENIED, true);
    EXPECT_TRUE(committer._hasSealError);
    EXPECT_FALSE(committer._canRefeshVersion);

    // 2. never call refreshVersion
    EXPECT_CALL(fileManager, refreshVersion()).Times(0);
    committer.setSealError(fslib::EC_PANGU_USER_DENIED, true);
    EXPECT_TRUE(committer._hasSealError);
    EXPECT_FALSE(committer._canRefeshVersion);

    // 3. call refreshVersion return ERROR_NONE
    EXPECT_CALL(fileManager, refreshVersion()).Times(1).WillOnce(Return(ERROR_NONE));
    committer._canRefeshVersion = true;
    committer.setSealError(fslib::EC_PANGU_USER_DENIED, true);
    EXPECT_FALSE(committer._hasSealError);
    EXPECT_TRUE(committer._canRefeshVersion);

    // 4. call refreshVersion return other error
    EXPECT_CALL(fileManager, refreshVersion()).Times(1).WillOnce(Return(ERROR_BROKER_FS_OPERATE));
    committer.setSealError(fslib::EC_PANGU_USER_DENIED, true);
    EXPECT_TRUE(committer._hasSealError);
    EXPECT_TRUE(committer._canRefeshVersion);
}

TEST_F(MessageCommitterTest, testOpenFileForWrite) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    PartitionId pid;
    config::PartitionConfig config;
    FileManager filemanager;
    string fileName("mock://na61/swift/test.data");
    fslib::ErrorCode ec = fslib::EC_FALSE;
    { // 1. not enable fast recover
        MessageCommitter commiter(pid, config, &filemanager, nullptr);
        EXPECT_FALSE(commiter._enableFastRecover);
        fslib::fs::File *ff = commiter.openFileForWrite(fileName, ec);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_TRUE(ff != nullptr);
        ff->close();
        DELETE_AND_SET_NULL(ff);
    }
    { // 2. enable fast recover, fail
        MessageCommitter commiter(pid, config, &filemanager, nullptr, true);
        EXPECT_TRUE(commiter._enableFastRecover);
        mockForward("pangu_OpenForWriteWithInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_FALSE;
                    });
        fslib::fs::File *ff = commiter.openFileForWrite(fileName, ec);
        EXPECT_EQ(fslib::EC_FALSE, ec);
        EXPECT_TRUE(ff != nullptr);
    }
    { // 3. enable fast recover, success
        MessageCommitter commiter(pid, config, &filemanager, nullptr, true);
        EXPECT_TRUE(commiter._enableFastRecover);
        mockForward("pangu_OpenForWriteWithInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_OK;
                    });
        fslib::fs::File *ff = commiter.openFileForWrite(fileName, ec);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_TRUE(ff != nullptr);
    }
}

} // namespace storage
} // namespace swift
