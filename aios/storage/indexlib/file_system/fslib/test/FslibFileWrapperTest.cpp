

#include "indexlib/file_system/fslib/FslibFileWrapper.h"

#include <iosfwd>
#include <stdint.h>
#include <string.h>
#include <string>
#include <utility>

#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "future_lite/Common.h"
#include "future_lite/Future.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FslibFileWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(FslibFileWrapperTest);
    void CaseSetUp() override
    {
        FslibWrapper::Reset();
        _rootDir = GET_TEMP_DATA_PATH();
        _notExistFile = _rootDir + "not_exist_file";
    }

    void CaseTearDown() override
    {
        bool ret = false;
        ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(_notExistFile, ret));
        if (ret) {
            ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(_notExistFile, DeleteOption::NoFence(false)).Code());
        }
        FslibWrapper::Reset();
    }

    void TestCaseForCloseTwice()
    {
        auto [ec, file] = FslibWrapper::OpenFile(_notExistFile, fslib::WRITE);
        ASSERT_EQ(FSEC_OK, ec);
        ASSERT_TRUE(file != NULL);
        ASSERT_EQ(FSEC_OK, file->Close());
        ASSERT_EQ(FSEC_OK, file->Close());
    }

    void checkWriteRead(size_t length, bool async = false)
    {
        bool ret = false;
        ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(_notExistFile, ret));
        if (ret) {
            ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(_notExistFile, DeleteOption::NoFence(false)).Code());
        }
        auto [ec, file] = FslibWrapper::OpenFile(_notExistFile, fslib::WRITE);
        ASSERT_EQ(FSEC_OK, ec);
        ASSERT_TRUE(file != NULL);
        char content[11] = "0123456789";
        char* inputBuf = new char[length];
        for (size_t i = 0; i < length; ++i) {
            inputBuf[i] = content[i % 10];
        }
        size_t realLength = 0;
        ASSERT_EQ(FSEC_OK, file->Write(inputBuf, length, realLength));
        ASSERT_EQ(FSEC_OK, file->Close());

        fslib::FileMeta fileMeta;
        ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(_notExistFile, fileMeta));
        ASSERT_EQ((int64_t)length, fileMeta.fileLength);

        file = FslibWrapper::OpenFile(_notExistFile, fslib::READ).GetOrThrow();
        ASSERT_TRUE(file != NULL);
        char* outputBuf = new char[length];
        memset(outputBuf, '0', length);
        ASSERT_EQ(FSEC_OK, file->Read(outputBuf, length, realLength));
        ASSERT_EQ(length, realLength);

        bool flag = true;
        for (size_t i = 0; i < length; i++) {
            flag &= (inputBuf[i] == outputBuf[i]);
        }
        ASSERT_TRUE(flag);
        ASSERT_EQ(FSEC_OK, file->Close());

        file = FslibWrapper::OpenFile(_notExistFile, fslib::READ).GetOrThrow();
        ASSERT_TRUE(file != NULL);
        memset(outputBuf, '0', length);
        size_t begingPos = 5 * 1024 * 1024;
        ASSERT_EQ(FSEC_OK, file->PRead(outputBuf, length - begingPos, begingPos, realLength));
        ASSERT_EQ(length - begingPos, realLength);
        flag = true;
        for (size_t i = begingPos; i < length; i++) {
            flag &= (inputBuf[i] == outputBuf[i - begingPos]);
        }
        ASSERT_TRUE(flag);
        ASSERT_EQ(FSEC_OK, file->Close());

        if (async) {
            file = FslibWrapper::OpenFile(_notExistFile, fslib::READ).GetOrThrow();
            ASSERT_TRUE(file != NULL);
            memset(outputBuf, '0', length);
            auto ret = file->PReadAsync(outputBuf, length - begingPos, begingPos, IO_ADVICE_LOW_LATENCY, nullptr).get();
            ASSERT_EQ(length - begingPos, ret.GetOrThrow());
            flag = true;
            for (size_t i = begingPos; i < length; i++) {
                flag &= (inputBuf[i] == outputBuf[i - begingPos]);
            }
            ASSERT_TRUE(flag);
            ASSERT_EQ(FSEC_OK, file->Close());
        }

        delete[] inputBuf;
        delete[] outputBuf;
        ASSERT_TRUE(flag);
        ret = false;
        ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(_notExistFile, ret));
        if (ret) {
            ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(_notExistFile, DeleteOption::NoFence(false)).Code());
        }
    }

    void TestCasForWriteRead()
    {
        size_t length = 64 * 1024 * 1024;
        checkWriteRead(length - 1);
        checkWriteRead(length, true);
        checkWriteRead(length + 1);
    }

private:
    string _rootDir;
    string _notExistFile;
};

INDEXLIB_UNIT_TEST_CASE(FslibFileWrapperTest, TestCaseForCloseTwice);
INDEXLIB_UNIT_TEST_CASE(FslibFileWrapperTest, TestCasForWriteRead);
}} // namespace indexlib::file_system
