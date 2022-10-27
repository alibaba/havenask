#include <autil/TimeUtility.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/test/data_flush_controller_unittest.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, DataFlushControllerTest);

DataFlushControllerTest::DataFlushControllerTest()
{
}

DataFlushControllerTest::~DataFlushControllerTest()
{
}

void DataFlushControllerTest::CaseSetUp()
{
    mTempFilePath = GET_TEST_DATA_PATH() + "/tmp";
}

void DataFlushControllerTest::CaseTearDown()
{
    FileSystemWrapper::DeleteIfExist(mTempFilePath);
}

void DataFlushControllerTest::TestInit()
{
    DataFlushController* controller = DataFlushController::GetInstance();
    controller->Reset();

    string param = "quota_size=4096,quota_interval=500,flush_unit=1024,force_flush=true";
    controller->InitFromString(param);

    ASSERT_EQ((uint32_t)4096, controller->mQuotaSize);
    ASSERT_EQ((uint32_t)1024, controller->mFlushUnit);
    ASSERT_EQ((uint32_t)500000, controller->mInterval);
    ASSERT_EQ(true, controller->mForceFlush);
}

void DataFlushControllerTest::TestReserveQuota()
{
    DataFlushController* controller = DataFlushController::GetInstance();
    controller->Reset();

    string param = "quota_size=4096,quota_interval=500,flush_unit=1024";
    controller->InitFromString(param);

    uint32_t count = 0;
    int64_t quota = 4096 * 5 + 1024;
    uint32_t expectCount = (quota + 1024 - 1) / 1024;

    uint32_t expectWaitCount = (quota + 4096 - 1) / 4096 - 1;
    uint32_t expectTimeInterval = 500 * 1000 * expectWaitCount;

    int64_t beginTs = TimeUtility::currentTime();
    while (quota > 0)
    {
        int64_t reserveQuota = controller->ReserveQuota(quota);
        quota -= reserveQuota;
        ++count;
    }
    int64_t endTs = TimeUtility::currentTime();
    ASSERT_EQ(expectCount, count);
    ASSERT_LT(expectTimeInterval, endTs - beginTs);
}

void DataFlushControllerTest::TestWriteFslibFile()
{
    DataFlushController* controller = DataFlushController::GetInstance();
    controller->Reset();

    string param = "quota_size=100,quota_interval=100,flush_unit=20,force_flush=true";
    controller->InitFromString(param);

    stringstream ss;
    for (size_t i = 0; i < 400; i++)
    {
        ss << i % 10 << endl;
    }
    string str = ss.str();

    FilePtr file(FileSystem::openFile(mTempFilePath, WRITE, false));
    controller->Write(file.get(), str.data(), str.size());
    file->close();

    string loadStr;
    FileSystemWrapper::AtomicLoad(mTempFilePath, loadStr);
    ASSERT_EQ(loadStr, str);
}

IE_NAMESPACE_END(storage);

