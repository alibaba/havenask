#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/write_control_file_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);

class WriteControlFileWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(WriteControlFileWrapperTest);
    void CaseSetUp() override
    {
        FileSystemWrapper::Reset();
        mRootDir = GET_TEST_DATA_PATH();
        mTestData = mRootDir + "write_control_file_wrapper_test";
    }

    void CaseTearDown() override
    {
        if (FileSystemWrapper::IsExist(mTestData))
        {
            FileSystemWrapper::DeleteFile(mTestData);
        }

        FileSystemWrapper::Reset();
    }

    void TestCaseForCheckFlush()
    {
        File* file = FileSystem::openFile(mTestData, fslib::WRITE, false);
        WriteControlFileWrapper *writeControlFileWrapper =
            new WriteControlFileWrapper(file, 100 * 1000, 64);

        int64_t lastFlushTime = writeControlFileWrapper->mLastFlushTime;
        writeControlFileWrapper->mUnFlushSize = 63;
        writeControlFileWrapper->CheckFlush();
        INDEXLIB_TEST_EQUAL(lastFlushTime,
                            writeControlFileWrapper->mLastFlushTime);
        INDEXLIB_TEST_EQUAL((int64_t)63,
                            writeControlFileWrapper->mUnFlushSize);
        
        writeControlFileWrapper->mUnFlushSize = 64;
        writeControlFileWrapper->CheckFlush();
        INDEXLIB_TEST_TRUE(lastFlushTime 
                            < writeControlFileWrapper->mLastFlushTime);
        INDEXLIB_TEST_EQUAL((int64_t)0,
                            writeControlFileWrapper->mUnFlushSize);
        writeControlFileWrapper->Close();
        delete writeControlFileWrapper;
    }

private:
    string mRootDir;
    string mTestData;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, WriteControlFileWrapperTest);

INDEXLIB_UNIT_TEST_CASE(WriteControlFileWrapperTest, TestCaseForCheckFlush);

IE_NAMESPACE_END(storage);
