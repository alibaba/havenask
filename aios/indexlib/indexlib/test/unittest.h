#ifndef __INDEXLIB_UNITTEST_H
#define __INDEXLIB_UNITTEST_H

//TODO: macros will dup with common/unittest.h

#include <string>
#include <typeinfo>
#define GTEST_USE_OWN_TR1_TUPLE 0
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <autil/StringUtil.h>

#define private public
#define protected public

#include "indexlib/test/test.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/io_exception_controller.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"
#include "indexlib/misc/singleton.h"

extern int INDEXLIB_TEST_ARGC;
extern char** INDEXLIB_TEST_ARGV;

#define DECLARE_CLASS_NAME(class_name)          \
    std::string GET_CLASS_NAME() const override { return #class_name; }

using testing::_;
using testing::Return;
using testing::ReturnRef;
using testing::Throw;
using testing::NiceMock;
using testing::SetArgReferee;
using testing::DoAll;
using testing::Invoke;
using testing::UnorderedElementsAre;
using testing::ElementsAre;
using testing::InSequence;

struct ScopedEnv
{
    ScopedEnv(const char* envName, const char* envValue) : _envName(envName) {
        setenv(envName, envValue, 1);
    }
    ~ScopedEnv() {
        unsetenv(_envName);
    }
    const char* _envName;
};

class INDEXLIB_TESTBASE_BASE
{
public:
    INDEXLIB_TESTBASE_BASE()
    {
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string progPath(INDEXLIB_TEST_ARGV[0]);
        printf("[          ] %s.%s [PROG=%s, PID=%d]\n",
               curTest->test_case_name(), curTest->name(),
               progPath.substr(progPath.rfind("/") + 1).c_str(), getpid());
        fflush(stdout);
    }
    virtual ~INDEXLIB_TESTBASE_BASE()
    {
    }

public:
    virtual void CaseSetUp() {}
    virtual void CaseTearDown() {}

    virtual std::string GET_CLASS_NAME() const
    { return typeid(*this).name(); }

    const std::string& GET_TEST_DATA_PATH() const
    { return mTestDataPath; }

    std::string GET_TEST_NAME() const
    {
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        return std::string(curTest->test_case_name()) + "." + curTest->name();
    }

    const IE_NAMESPACE(file_system)::DirectoryPtr&
    GET_PARTITION_DIRECTORY() const { return mPartitionDirectory; }

    IE_NAMESPACE(file_system)::DirectoryPtr GET_SEGMENT_DIRECTORY()
    {
        static std::string TEST_SEGMENT_DIRNAME = "test_segment";
        if (!mPartitionDirectory->IsExist(TEST_SEGMENT_DIRNAME))
        {
            mSegmentDirectory = mPartitionDirectory->MakeInMemDirectory(
                    TEST_SEGMENT_DIRNAME);
        }
        else
        {
            mSegmentDirectory = mPartitionDirectory->GetDirectory(
                    TEST_SEGMENT_DIRNAME, true);
        }
        return mSegmentDirectory;
    }

    const IE_NAMESPACE(file_system)::IndexlibFileSystemPtr&
    GET_FILE_SYSTEM() const { return mFileSystem; }

    IE_NAMESPACE(file_system)::DirectoryPtr MAKE_SEGMENT_DIRECTORY(
            segmentid_t segId)
    {
        IE_NAMESPACE(file_system)::DirectoryPtr dir =
            mPartitionDirectory->MakeInMemDirectory(std::string("segment_") +
                    autil::StringUtil::toString(segId) + "_level_0");
        if (mFileSystem)
        {
            mFileSystem->Sync(true);
        }
        return dir;
    }

    void RESET_FILE_SYSTEM(const IE_NAMESPACE(config)::LoadConfigList& loadConfigList,
                           bool isAsync = false, bool useCache = true,
                           bool needFlush = true, bool useRootLink = false,
                           const std::string& secondaryPath = "", bool isOffline = false)
    {
        if (mFileSystem)
        {
            try
            {
                mFileSystem->Sync(true);
            }
            catch(const IE_NAMESPACE(misc)::ExceptionBase& exception)
            {}
        }
        mFileSystem.reset();
        IE_NAMESPACE(file_system)::FileSystemOptions options;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = isAsync;
        options.needFlush = needFlush;
        options.useCache = useCache;
        options.useRootLink = useRootLink;
        options.isOffline = isOffline;
        mFileSystem = CreateFileSystem(options, secondaryPath);
    }

    void INIT_FORMAT_VERSION_FILE(const std::string& formatVersion)
    {
        IE_NAMESPACE(file_system)::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
        assert(rootDirectory);
        if (rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME))
        {
            rootDirectory->RemoveFile(INDEX_FORMAT_VERSION_FILE_NAME);
        }

        IE_NAMESPACE(index_base)::IndexFormatVersion indexFormatVersion(formatVersion);
        indexFormatVersion.Store(rootDirectory);
    }

    std::string GET_ABS_PATH(const std::string& relativePath)
    {
        return IE_NAMESPACE(util)::PathUtil::JoinPath(
                mFileSystem->GetRootPath(), relativePath);
    }
protected:
    void TestDataPathSetup()
    {
        typedef IE_NAMESPACE(storage)::FileSystemWrapper FileSystemWrapper;
        std::string tempPath = FileSystemWrapper::JoinPath(
                TEST_DATA_PATH, GET_CLASS_NAME());
        mTestDataPath = FileSystemWrapper::NormalizeDir(tempPath);

        DeleteExistDataPath();
        FileSystemWrapper::MkDir(mTestDataPath, true);

        IE_NAMESPACE(config)::LoadConfigList loadConfigList;
        RESET_FILE_SYSTEM(loadConfigList, false);
    }

    void TestDataPathTearDown()
    {
        mSegmentDirectory.reset();
        mPartitionDirectory.reset();
        if (mFileSystem)
        {
            mFileSystem->Sync(true);
            mFileSystem.reset();
        }
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        const ::testing::TestResult* result = curTest->result();
        if (result->Passed() && !getenv("INDEXLIB_UT_NOT_DELETE_TEST_PATH"))
        {
            DeleteExistDataPath();
        }
    }

    IE_NAMESPACE(file_system)::IndexlibFileSystemPtr CreateFileSystem(
            IE_NAMESPACE(file_system)::FileSystemOptions options,
            const std::string& secondaryRootPath = "")
    {
        IE_NAMESPACE(file_system)::IndexlibFileSystemPtr fileSystem(
                new IE_NAMESPACE(file_system)::IndexlibFileSystemImpl(mTestDataPath, secondaryRootPath));
        IE_NAMESPACE(util)::MemoryQuotaControllerPtr quotaControllor(
                new IE_NAMESPACE(util)::MemoryQuotaController(1024*1024*1024));
        IE_NAMESPACE(util)::PartitionMemoryQuotaControllerPtr controllor(
                new IE_NAMESPACE(util)::PartitionMemoryQuotaController(quotaControllor));
        options.memoryQuotaController = controllor;;
        fileSystem->Init(options);

        mPartitionDirectory = IE_NAMESPACE(file_system)::DirectoryCreator::Get(
                fileSystem, mTestDataPath, true);
        return fileSystem;
    }

private:
    void DeleteExistDataPath()
    {
        if (!mTestDataPath.empty()
            && mTestDataPath.rfind(TEST_DATA_PATH) != std::string::npos
            && IE_NAMESPACE(storage)::FileSystemWrapper::IsExist(mTestDataPath))
        {
            IE_NAMESPACE(storage)::FileSystemWrapper::DeleteDir(mTestDataPath);
        }
    }

private:
    std::string mTestDataPath;
    IE_NAMESPACE(file_system)::DirectoryPtr mPartitionDirectory;
    IE_NAMESPACE(file_system)::DirectoryPtr mSegmentDirectory;
    IE_NAMESPACE(file_system)::IndexlibFileSystemPtr mFileSystem;

};

class INDEXLIB_TESTBASE : public INDEXLIB_TESTBASE_BASE, public testing::Test
{
public:
    void SetUp()
    {
        setenv("KV_SUPPORT_SWAP_MMAP_FILE", "true", 1);
        setenv("INDEXLIB_TEST_MERGE_CHECK_POINT", "true", 1);

        TestDataPathTearDown();
        TestDataPathSetup();
        IE_NAMESPACE(misc)::IoExceptionController::SetFileIOExceptionStatus(false);
        CaseSetUp();
    }

    void TearDown()
    {
        CaseTearDown();
        TestDataPathTearDown();
    }
};

#define INDEXLIB_UNIT_TEST_CASE(className, caseName)  \
    TEST_F(className, caseName) {                     \
        caseName();                                   \
    }

#define DISABLED_INDEXLIB_UNIT_TEST_CASE(className, caseName)           \
    TEST_F(className, DISABLED_##caseName) {                            \
        caseName();                                                     \
    }

template <class T>
class INDEXLIB_TESTBASE_WITH_PARAM : public INDEXLIB_TESTBASE_BASE,
                                     public testing::TestWithParam<T>
{
public:
    void SetUp()
    {
        setenv("KV_SUPPORT_SWAP_MMAP_FILE", "true", 1);
        setenv("INDEXLIB_TEST_MERGE_CHECK_POINT", "true", 1);

        TestDataPathTearDown();
        TestDataPathSetup();
        CaseSetUp();
    }

    void TearDown()
    {
        CaseTearDown();
        TestDataPathTearDown();
    }

};

#define GET_CASE_PARAM() GetParam()

#define INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(className, caseName)         \
    TEST_P(className, caseName)                                         \
    {                                                                   \
        caseName();                                                     \
    }

#define INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(name, className, param)    \
    class name : public className {};                                   \
INSTANTIATE_TEST_CASE_P(className, name, param)

#define INDEXLIB_INSTANTIATE_PARAM_NAME(name, className)        \
    class name : public className {};                           \

#define INDEXLIB_TEST_TRUE(x) ASSERT_TRUE(x)
#define INDEXLIB_TEST_EQUAL(x, y) ASSERT_EQ(x, y)
#define INDEXLIB_EXPECT_EXCEPTION(x, y) ASSERT_THROW(x, y)

#define GET_PARAM_VALUE(idx) std::tr1::get<(idx)>(GET_CASE_PARAM())

#endif //__INDEXLIB_UNITTEST_H
