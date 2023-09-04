#include "indexlib/file_system/fslib/DataFlushController.h"

#include "gtest/gtest.h"
#include <memory>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {

class DataFlushControllerTest : public INDEXLIB_TESTBASE
{
public:
    DataFlushControllerTest();
    ~DataFlushControllerTest();

    DECLARE_CLASS_NAME(DataFlushControllerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestReserveQuota();
    void TestWriteFslibFile();

private:
    std::string _tempFilePath;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, DataFlushControllerTest);

INDEXLIB_UNIT_TEST_CASE(DataFlushControllerTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(DataFlushControllerTest, TestReserveQuota);
INDEXLIB_UNIT_TEST_CASE(DataFlushControllerTest, TestWriteFslibFile);

//////////////////////////////////////////////////////////////////////

DataFlushControllerTest::DataFlushControllerTest() {}

DataFlushControllerTest::~DataFlushControllerTest() {}

void DataFlushControllerTest::CaseSetUp()
{
    _tempFilePath = util::PathUtil::NormalizePath(GET_TEMP_DATA_PATH()) + "/tmp";
}

void DataFlushControllerTest::CaseTearDown() {}

void DataFlushControllerTest::TestInit()
{
    auto* multiPathController = MultiPathDataFlushController::GetInstance();
    multiPathController->Clear();

    string param = "quota_size=4096,quota_interval=500,flush_unit=1024,force_flush=true";
    multiPathController->InitFromString(param);
    auto* controller = multiPathController->GetDataFlushController("");
    ASSERT_EQ((uint32_t)4096, controller->_quotaSize);
    ASSERT_EQ((uint32_t)1024, controller->_flushUnit);
    ASSERT_EQ((uint32_t)500000, controller->_interval);
    ASSERT_TRUE(controller->_forceFlush);
}

void DataFlushControllerTest::TestReserveQuota()
{
    auto* multiPathController = MultiPathDataFlushController::GetInstance();
    multiPathController->Clear();

    string param = "quota_size=4096,quota_interval=500,flush_unit=1024";
    multiPathController->InitFromString(param);
    auto* controller = multiPathController->GetDataFlushController("");

    uint32_t count = 0;
    int64_t quota = 4096 * 5 + 1024;
    uint32_t expectCount = (quota + 1024 - 1) / 1024;

    uint32_t expectWaitCount = (quota + 4096 - 1) / 4096 - 1;
    uint32_t expectTimeInterval = 500 * 1000 * expectWaitCount;

    int64_t beginTs = TimeUtility::currentTime();
    while (quota > 0) {
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
    auto* multiPathController = MultiPathDataFlushController::GetInstance();
    multiPathController->Clear();

    string param = "quota_size=4096,quota_interval=500,flush_unit=1024";
    multiPathController->InitFromString(param);
    auto* controller = multiPathController->GetDataFlushController("");
    stringstream ss;
    for (size_t i = 0; i < 400; i++) {
        ss << i % 10 << endl;
    }
    string str = ss.str();

    fslib::fs::FilePtr file(fslib::fs::FileSystem::openFile(_tempFilePath, fslib::WRITE, false));
    ASSERT_EQ(FSEC_OK, controller->Write(file.get(), str.data(), str.size()).Code());
    file->close();

    string loadStr;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(_tempFilePath, loadStr));
    ASSERT_EQ(loadStr, str);
}
}} // namespace indexlib::file_system
