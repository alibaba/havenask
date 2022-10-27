#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/test/test.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);

class FileWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(FileWrapperTest);
    void CaseSetUp() override
    {
        FileSystemWrapper::Reset();
        mRootDir = GET_TEST_DATA_PATH();
        mNotExistFile = mRootDir + "not_exist_file";
    }

    void CaseTearDown() override
    {
        if (FileSystemWrapper::IsExist(mNotExistFile))
        {
            FileSystemWrapper::DeleteFile(mNotExistFile);
        }
        FileSystemWrapper::Reset();
    }

    void TestCaseForCloseTwice()
    {
        FileWrapper* file = FileSystemWrapper::OpenFile(mNotExistFile, fslib::WRITE);
        INDEXLIB_TEST_TRUE(file != NULL);
        file->Close();
        file->Close();
        delete file;  
    }

    void checkWriteRead(size_t length) {
        if (FileSystemWrapper::IsExist(mNotExistFile))
        {
            FileSystemWrapper::DeleteFile(mNotExistFile);
        }
        FileWrapper* file = FileSystemWrapper::OpenFile(mNotExistFile, fslib::WRITE);
        INDEXLIB_TEST_TRUE(file != NULL);
        char content[11] = "0123456789";
        char *inputBuf = new char[length];
        for (size_t i = 0; i < length; ++i) 
        {
            inputBuf[i] = content[i % 10];
        }
        file->Write(inputBuf, length);
        file->Close();
        delete file;

        fslib::FileMeta fileMeta;
        FileSystemWrapper::GetFileMeta(mNotExistFile, fileMeta);
        INDEXLIB_TEST_EQUAL((int64_t)length, fileMeta.fileLength);

        file = FileSystemWrapper::OpenFile(mNotExistFile, fslib::READ);
        INDEXLIB_TEST_TRUE(file != NULL);
        char *outputBuf = new char[length];
        memset(outputBuf, '0', length);
        INDEXLIB_TEST_EQUAL(length, file->Read(outputBuf, length));

        bool flag = true;
        for (size_t i = 0; i < length; i++)
        {
            flag &= (inputBuf[i] == outputBuf[i]);
        }
        INDEXLIB_TEST_TRUE(flag);
        file->Close();
        delete file;

        file = FileSystemWrapper::OpenFile(mNotExistFile, fslib::READ);
        INDEXLIB_TEST_TRUE(file != NULL);
        memset(outputBuf, '0', length);
        size_t begingPos = 5 * 1024 * 1024;
        INDEXLIB_TEST_EQUAL(length - begingPos, file->PRead(outputBuf, 
                        length - begingPos, begingPos));
        flag = true;
        for (size_t i = begingPos; i < length; i++) 
        {
            flag &= (inputBuf[i] == outputBuf[i - begingPos]);
        }
        delete [] inputBuf;
        delete [] outputBuf;
        INDEXLIB_TEST_TRUE(flag);
        file->Close();
        delete file;
        if (FileSystemWrapper::IsExist(mNotExistFile))
        {
            FileSystemWrapper::DeleteFile(mNotExistFile);
        }
    }
    
    void TestCasForWriteRead()
    {
        size_t length = 64 * 1024 * 1024; 
        checkWriteRead(length - 1);
        checkWriteRead(length);
        checkWriteRead(length + 1);
    }

private:
    string mRootDir;
    string mNotExistFile;
};

INDEXLIB_UNIT_TEST_CASE(FileWrapperTest, TestCaseForCloseTwice);
INDEXLIB_UNIT_TEST_CASE(FileWrapperTest, TestCasForWriteRead);

IE_NAMESPACE_END(storage);
