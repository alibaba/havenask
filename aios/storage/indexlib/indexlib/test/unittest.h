#ifndef __INDEXLIB_UNITTEST_H
#define __INDEXLIB_UNITTEST_H

// TODO: macros will dup with common/unittest.h

#include <string>
#include <typeinfo>
#define GTEST_USE_OWN_TR1_TUPLE 0
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <unittest/unittest.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/PhysicalDirectory.h"

extern int TEST_ARGC;
extern char** TEST_ARGV;

using testing::ElementsAre;
using testing::InSequence;
using testing::NiceMock;
using testing::ReturnRef;
using testing::UnorderedElementsAre;

class INDEXLIB_TESTBASE_BASE : public TESTBASE_BASE
{
public:
    INDEXLIB_TESTBASE_BASE(bool needFileSystem = true) : TESTBASE_BASE(), mNeedFileSystem(needFileSystem)
    {
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string progPath(TEST_ARGV[0]);
        printf("[          ] %s.%s [PROG=%s, PID=%d]\n", curTest->test_case_name(), curTest->name(),
               progPath.substr(progPath.rfind("/") + 1).c_str(), getpid());
        fflush(stdout);
        mTestDataPath = TESTBASE_BASE::TEST_ROOT_PATH() + "indexlib/testdata/";
    }
    ~INDEXLIB_TESTBASE_BASE() {}

public:
    // for ut, call by setUp & tearDown
    virtual void CaseSetUp() {}
    virtual void CaseTearDown() {}
    virtual std::string GET_CLASS_NAME() const { return typeid(*this).name(); }

public:
    std::string GET_TEST_NAME() const
    {
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        return std::string(curTest->test_case_name()) + "." + curTest->name();
    }

    // read only, "testdata/"
    const std::string& GET_TEST_DATA_PATH() const { return mTestDataPath; }
    // writable, tmpdir
    const std::string& GET_TEMP_DATA_PATH() const { return TESTBASE_BASE::GET_TEMP_DATA_PATH(); }
    std::string GET_TEMP_DATA_PATH(const std::string& relativePath) const
    {
        return TESTBASE_BASE::GET_TEMP_DATA_PATH() + relativePath;
    }
    std::string GET_NON_RAMDISK_PATH() const { return "/tmp/" + GET_TEMP_DATA_PATH(); }

public:
    indexlib::file_system::DirectoryPtr GET_CHECK_DIRECTORY(bool isReadonly = true) const
    {
        return indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    }

    // TODO: rm, for FileSystem
    const indexlib::file_system::DirectoryPtr& GET_PARTITION_DIRECTORY() const { return mPartitionDirectory; }

    indexlib::file_system::DirectoryPtr GET_SEGMENT_DIRECTORY()
    {
        static std::string TEST_SEGMENT_DIRNAME = "test_segment";
        if (!mPartitionDirectory->IsExist(TEST_SEGMENT_DIRNAME)) {
            mSegmentDirectory =
                mPartitionDirectory->MakeDirectory(TEST_SEGMENT_DIRNAME, indexlib::file_system::DirectoryOption::Mem());
        } else {
            mSegmentDirectory = mPartitionDirectory->GetDirectory(TEST_SEGMENT_DIRNAME, true);
        }
        return mSegmentDirectory;
    }

    const indexlib::file_system::IFileSystemPtr& GET_FILE_SYSTEM() const { return mFileSystem; }

    indexlib::file_system::DirectoryPtr MAKE_SEGMENT_DIRECTORY(int32_t segId)
    {
        indexlib::file_system::DirectoryPtr dir = mPartitionDirectory->MakeDirectory(
            std::string("segment_") + autil::StringUtil::toString(segId) + "_level_0",
            indexlib::file_system::DirectoryOption::Mem());
        if (mFileSystem) {
            mFileSystem->Sync(true).GetOrThrow();
        }
        return dir;
    }

    // TODO: remove
    void RESET_FILE_SYSTEM(
        const std::string& name = "ut", bool autoMount = true,
        const indexlib::file_system::FileSystemOptions& fsOptions = indexlib::file_system::FileSystemOptions::Offline())
    {
        mFileSystem =
            indexlib::file_system::FileSystemCreator::Create(name, GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
        mPartitionDirectory = indexlib::file_system::Directory::Get(mFileSystem);
    }

    void RESET_FILE_SYSTEM(const indexlib::file_system::LoadConfigList& loadConfigList, bool isAsync = false,
                           bool useCache = true, bool needFlush = true, bool useRootLink = false,
                           bool isOffline = false)
    {
        if (mFileSystem) {
            mFileSystem.reset();
        }
        indexlib::file_system::FileSystemOptions options;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = isAsync;
        options.needFlush = needFlush;
        options.useCache = useCache;
        options.useRootLink = useRootLink;
        options.isOffline = isOffline;

        mFileSystem = CreateFileSystem(options);
        mPartitionDirectory = indexlib::file_system::Directory::Get(mFileSystem);
    }

protected:
    // derived from TESTBASE_BASE
    void testDataPathSetup() override final
    {
        TESTBASE_BASE::testDataPathSetup();
        indexlib::file_system::LoadConfigList loadConfigList;
        if (mNeedFileSystem) {
            RESET_FILE_SYSTEM(loadConfigList, false);
        }
    }

    void testDataPathTearDown() override final
    {
        mSegmentDirectory.reset();
        mPartitionDirectory.reset();
        if (mFileSystem) {
            mFileSystem->Sync(true).GetOrThrow();
            mFileSystem.reset();
        }
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        const ::testing::TestResult* result = curTest->result();
        if (result->Passed() && !getenv("INDEXLIB_UT_NOT_DELETE_TEST_PATH")) {
            TESTBASE_BASE::testDataPathTearDown();
        }
        std::string nonRamDiskPath = GET_NON_RAMDISK_PATH();
        if (nonRamDiskPath.rfind(_testTempRoot) != std::string::npos && access(nonRamDiskPath.c_str(), F_OK) == 0) {
            remove(nonRamDiskPath);
        }
    }
    bool needCheckCase()
    {
        const char* envStr = getenv("CHECK_CASE_TIMEOUT");
        if (envStr) {
            if (std::string(envStr) == "true") {
                return true;
            }
        }
        return false;
    }

protected:
    // derived from TESTBASE_BASE, call by testing::Test::SetUp & testing::Test::TearDown
    void setUp() override final
    {
        setenv("KV_SUPPORT_SWAP_MMAP_FILE", "true", 1);
        setenv("INDEXLIB_TEST_MERGE_CHECK_POINT", "true", 1);
        setenv("IS_INDEXLIB_TEST_MODE", "true", 1);
        setenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE", "true", 1);
        testDataPathTearDown();
        testDataPathSetup();
        CaseSetUp();
    }
    void tearDown() override final
    {
        CaseTearDown();
        testDataPathTearDown();
        unsetenv("KV_SUPPORT_SWAP_MMAP_FILE");
        unsetenv("INDEXLIB_TEST_MERGE_CHECK_POINT");
        unsetenv("IS_INDEXLIB_TEST_MODE");
        unsetenv("TEST_QUICK_EXIT");
        unsetenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE");
    }

private:
    indexlib::file_system::IFileSystemPtr CreateFileSystem(indexlib::file_system::FileSystemOptions options)
    {
        auto fs = indexlib::file_system::FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
        mPartitionDirectory = indexlib::file_system::Directory::Get(fs);
        if (mFileSystem) {
            EXPECT_EQ(indexlib::file_system::FSEC_OK,
                      fs->MountDir(GET_TEMP_DATA_PATH(), "", "", indexlib::file_system::FSMT_READ_WRITE, true));
        }
        return fs;
    }

protected:
    indexlib::file_system::DirectoryPtr mPartitionDirectory;
    indexlib::file_system::DirectoryPtr mSegmentDirectory;
    indexlib::file_system::IFileSystemPtr mFileSystem;
    bool mNeedFileSystem = true;
    std::string mTestDataPath;
    int64_t mCaseBeginTime;
};

//////////////////////////////////////////////////////////////////////
class INDEXLIB_TESTBASE : public INDEXLIB_TESTBASE_BASE, public testing::Test
{
private:
    int vsystem(const char* cmd)
    {
        const char* new_argv[4];
        new_argv[0] = "sh";
        new_argv[1] = "-c";
        new_argv[2] = (cmd == nullptr) ? "exit 0" : cmd;
        new_argv[3] = nullptr;
        pid_t pid;
        int status;

        pid = vfork();
        if (pid == 0) {
            // child
            execv("/bin/sh", const_cast<char* const*>(new_argv));
        } else if (pid < 0) {
            // error
            return -1;
        } else {
            // parent
            waitpid(pid, &status, 0);
            return (cmd == nullptr) ? (status == 0) : status;
        }
        assert(false);
        return -1;
    }

public:
    INDEXLIB_TESTBASE(bool needFileSystem = true) : INDEXLIB_TESTBASE_BASE(needFileSystem) {}

    void prepareCodegenEnv()
    {
        std::string workdir = std::string(GET_PRIVATE_TEST_DATA_PATH()) + "/codegen_dir/";
        if (access(workdir.c_str(), F_OK) != 0) {
            mkdir(workdir.c_str(), 0755);
            std::string codegenTar = std::string(GET_PRIVATE_TEST_DATA_PATH()) + "/codegen_package.tar";
            std::string codegenHeaderTar = std::string(GET_PRIVATE_TEST_DATA_PATH()) + "/indexlib_codegen_header.tar";
            std::string cmd;
            if (access(codegenTar.c_str(), F_OK) == 0) {
                cmd = "tar -xf " + codegenTar + " -C " + workdir;
                vsystem(cmd.c_str());
            }
            if (access(codegenHeaderTar.c_str(), F_OK) == 0) {
                cmd = "tar -xf " + codegenHeaderTar + " -C " + workdir;
                vsystem(cmd.c_str());
            }
        }
        setenv("HIPPO_APP_WORKDIR", workdir.c_str(), 1);
        std::string installRoot = std::string(GET_PRIVATE_TEST_DATA_PATH()) + "/codegen_dir/";
        setenv("HIPPO_APP_INST_ROOT", installRoot.c_str(), 1);
        setenv("codegen_compile_thread_num", "20", 1);
        const char* path = getenv("PATH");
        std::string pathStr;
        if (path) {
            pathStr = std::string(path);
        }
        pathStr = pathStr + ":" + installRoot + "/usr/local/bin/";
        setenv("PATH", pathStr.c_str(), 1);
    }

private:
    // derived from testing::Test, call by gtest frame
    void SetUp() override final
    {
        if (needCheckCase()) {
            mCaseBeginTime = autil::TimeUtility::currentTimeInSeconds();
        }
        setUp();
    }
    void TearDown() override final
    {
        if (needCheckCase()) {
            int64_t endTime = autil::TimeUtility::currentTimeInSeconds();
            ASSERT_TRUE(endTime - mCaseBeginTime <= 10) << "case timeout";
        }
        tearDown();
    }

private:
    friend class testing::Test;
};

//////////////////////////////////////////////////////////////////////
template <class T>
class INDEXLIB_TESTBASE_WITH_PARAM : public INDEXLIB_TESTBASE_BASE, public testing::TestWithParam<T>
{
private:
    void SetUp() override final
    {
        if (needCheckCase()) {
            mCaseBeginTime = autil::TimeUtility::currentTimeInSeconds();
        }
        setUp();
    }
    void TearDown() override final
    {
        if (needCheckCase()) {
            int64_t endTime = autil::TimeUtility::currentTimeInSeconds();
            ASSERT_TRUE(endTime - mCaseBeginTime <= 10) << "case timeout";
        }
        tearDown();
    }

private:
    friend class testing::TestWithParam<T>;
};

//////////////////////////////////////////////////////////////////////
#define DECLARE_CLASS_NAME(class_name)                                                                                 \
    std::string GET_CLASS_NAME() const override { return #class_name; }

#define INDEXLIB_UNIT_TEST_CASE(className, caseName)                                                                   \
    TEST_F(className, caseName) { caseName(); }

#define DISABLED_INDEXLIB_UNIT_TEST_CASE(className, caseName)                                                          \
    TEST_F(className, DISABLED_##caseName) { caseName(); }

#define INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(className, caseName)                                                        \
    TEST_P(className, caseName) { caseName(); }

#define INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(name, className, param)                                                   \
    class name : public className                                                                                      \
    {                                                                                                                  \
    };                                                                                                                 \
    INSTANTIATE_TEST_CASE_P(className, name, param)

#define INDEXLIB_INSTANTIATE_PARAM_NAME(name, className)                                                               \
    class name : public className                                                                                      \
    {                                                                                                                  \
    };

#define INDEXLIB_TEST_TRUE(x) ASSERT_TRUE(x)
#define INDEXLIB_TEST_EQUAL(x, y) ASSERT_EQ(x, y)
#define INDEXLIB_EXPECT_EXCEPTION(x, y) ASSERT_THROW(x, y)

#define GET_CASE_PARAM() GetParam()
#define GET_PARAM_VALUE(idx) std::get<(idx)>(GET_CASE_PARAM())

#endif //__INDEXLIB_UNITTEST_H
