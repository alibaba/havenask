#include "swift/util/PanguInlineFileUtil.h"

#include <functional>
#include <iosfwd>
#include <stdint.h>
#include <stdlib.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/DllWrapper.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemFactory.h"
#include "unittest/unittest.h"

namespace fslib {
namespace fs {
class File;
} // namespace fs
} // namespace fslib

using namespace std;
using namespace autil;
using namespace fslib::fs;

namespace swift {
namespace util {
#define FSLIB_MOCK_FORWARD_FUNCTION_NAME "mockForward"
class PanguInlineFileUtilTest : public TESTBASE {
public:
protected:
};

TEST_F(PanguInlineFileUtilTest, testInlineVersionSimple) {
    {
        InlineVersion inlineVersion;
        ASSERT_FALSE(inlineVersion.valid());
    }
    {
        InlineVersion inlineVersion(1, 2);
        EXPECT_EQ(1, inlineVersion.getMasterVersion());
        EXPECT_EQ(2, inlineVersion.getPartVersion());
        ASSERT_TRUE(inlineVersion.valid());
    }
}

TEST_F(PanguInlineFileUtilTest, testInlineVersionDeserializeVersionError) {
    InlineVersion inlineVersion;
    std::string inlineVersionStr = "0123456789abcde"; // 15 chars
    ASSERT_FALSE(inlineVersion.deserialize(inlineVersionStr));
}

TEST_F(PanguInlineFileUtilTest, testInlineVersionDeserializeSimple) {
    {
        InlineVersion inlineVersion;
        std::string inlineVersionStr(20, char(0));
        ASSERT_TRUE(inlineVersion.deserialize(inlineVersionStr));
        EXPECT_EQ(0, inlineVersion.getMasterVersion());
        EXPECT_EQ(0, inlineVersion.getPartVersion());
    }
    {
        InlineVersion inlineVersion;
        std::string inlineVersionStr(20, char(255));
        ASSERT_TRUE(inlineVersion.deserialize(inlineVersionStr));
        ASSERT_TRUE(0xFFFFFFFFFFFFFFFF == inlineVersion.getMasterVersion());
        ASSERT_TRUE(0xFFFFFFFFFFFFFFFF == inlineVersion.getPartVersion());
    }
    {
        uint64_t partVersion = 1668137488000000;
        uint64_t masterVersion = 1668137488123456;
        InlineVersion inlineVersion(masterVersion, partVersion);
        string serStr = inlineVersion.serialize();
        InlineVersion inlineVersion2;
        ASSERT_TRUE(inlineVersion2.deserialize(serStr));
        EXPECT_EQ(serStr, inlineVersion2.serialize());
        EXPECT_EQ(inlineVersion.getPartVersion(), inlineVersion2.getPartVersion());
        EXPECT_EQ(inlineVersion.getMasterVersion(), inlineVersion2.getMasterVersion());
        EXPECT_EQ(inlineVersion, inlineVersion2);
    }
}

TEST_F(PanguInlineFileUtilTest, testInlineVersionOperator) {
    uint64_t partVersion = 1668137488000000;
    uint64_t masterVersion = 1668137488123456;
    ASSERT_FALSE(InlineVersion(masterVersion, partVersion) < InlineVersion(masterVersion, partVersion));
    ASSERT_TRUE(InlineVersion(masterVersion, partVersion) < InlineVersion(masterVersion + 1, partVersion));
    ASSERT_TRUE(InlineVersion(masterVersion, partVersion) < InlineVersion(masterVersion, partVersion + 1));
    ASSERT_FALSE(InlineVersion(masterVersion + 1, partVersion) < InlineVersion(masterVersion, partVersion + 1));
    ASSERT_TRUE(InlineVersion(masterVersion, partVersion) == InlineVersion(masterVersion, partVersion));
    ASSERT_FALSE(InlineVersion(masterVersion, partVersion) == InlineVersion(masterVersion, partVersion + 1));
}

typedef std::function<fslib::ErrorCode(const std::string &path, const std::string &args, std::string &output)>
    ForwardFunc;
typedef void (*MockForwardFunc)(const std::string &, const ForwardFunc &);

TEST_F(PanguInlineFileUtilTest, testIsInlineFileExist) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    string mockInline("mock://na61/swift/inline");
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_OK;
                    });
        std::string output;
        bool isExist = false;
        fslib::ErrorCode ec = PanguInlineFileUtil::isInlineFileExist(mockInline, isExist);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_TRUE(isExist);
    }
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_NOENT;
                    });
        std::string output;
        bool isExist = false;
        fslib::ErrorCode ec = PanguInlineFileUtil::isInlineFileExist(mockInline, isExist);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_FALSE(isExist);
    }
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_NOENT;
                    });
        std::string output;
        bool isExist = false;
        fslib::ErrorCode ec = PanguInlineFileUtil::isInlineFileExist(mockInline, isExist);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_FALSE(isExist);
    }
}

TEST_F(PanguInlineFileUtilTest, testOpenFileForWrite) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    string fileName("mock://na61/swift/test.data");
    fslib::ErrorCode ec = fslib::EC_FALSE;
    {
        mockForward("pangu_OpenForWriteWithInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        const string &expectArgs = StringUtil::formatString("inline%c1668137488000000", '\0');
                        output = "100";
                        return (args == expectArgs) ? fslib::EC_OK : fslib::EC_FALSE;
                    });
        fslib::fs::File *ff = PanguInlineFileUtil::openFileForWrite(fileName, "inline", "1668137488000000", ec);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_TRUE(ff != nullptr);
        EXPECT_EQ((void *)100, ff);
        ff = PanguInlineFileUtil::openFileForWrite(fileName, "inline2", "1668137488000000", ec);
        EXPECT_EQ(fslib::EC_FALSE, ec);
        EXPECT_TRUE(ff != nullptr);
        EXPECT_EQ((void *)100, ff);
    }
}

TEST_F(PanguInlineFileUtilTest, testGetInlineFile) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    string mockInline("mock://na61/swift/inline");
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_OK;
                    });
        std::string output;
        fslib::ErrorCode ec = PanguInlineFileUtil::getInlineFile(mockInline, output);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_EQ("100", output);
    }
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_FALSE;
                    });
        std::string output;
        fslib::ErrorCode ec = PanguInlineFileUtil::getInlineFile(mockInline, output);
        EXPECT_EQ(fslib::EC_FALSE, ec);
    }
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_NOENT;
                    });
        mockForward("pangu_CreateInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "101";
                        return fslib::EC_OK;
                    });
        std::string output;
        fslib::ErrorCode ec = PanguInlineFileUtil::getInlineFile(mockInline, output);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_EQ("101", output);
    }
}

TEST_F(PanguInlineFileUtilTest, testUpdateInlineFile) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    string mockInline("mock://na61/swift/inline");
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_UpdateInlinefileCAS",
                [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                    const string &expectArgs = StringUtil::formatString("1234%c5678", '\0');
                    output = "100";
                    return (args == expectArgs) ? fslib::EC_OK : fslib::EC_FALSE;
                });
    fslib::ErrorCode ec = PanguInlineFileUtil::updateInlineFile(mockInline, 1234, 5678);
    EXPECT_EQ(fslib::EC_OK, ec);
    ec = PanguInlineFileUtil::updateInlineFile(mockInline, 2345, 5678);
    EXPECT_EQ(fslib::EC_FALSE, ec);
}

TEST_F(PanguInlineFileUtilTest, testSealFile) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    string mockInline("mock://na61/swift/inline");
    {
        mockForward("pangu_SealFile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
            output = "100";
            return fslib::EC_OK;
        });
        fslib::ErrorCode ec = PanguInlineFileUtil::sealFile(mockInline);
        EXPECT_EQ(fslib::EC_OK, ec);
    }
    {
        mockForward("pangu_SealFile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
            output = "100";
            return fslib::EC_NOENT;
        });
        fslib::ErrorCode ec = PanguInlineFileUtil::sealFile(mockInline);
        EXPECT_EQ(fslib::EC_NOENT, ec);
    }
}

} // namespace util
} // namespace swift
