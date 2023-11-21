#include "indexlib/file_system/file/SliceFileNode.h"

#include "gtest/gtest.h"
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unistd.h>

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class UnSupportedException;
}} // namespace indexlib::util
using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class SliceFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    SliceFileNodeTest();
    ~SliceFileNodeTest();

    DECLARE_CLASS_NAME(SliceFileNodeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSliceFileReaderNotSupport();
    void TestDoNotUseRSSBeforeWrite();

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, SliceFileNodeTest);

// INDEXLIB_UNIT_TEST_CASE(SliceFileNodeTest, TestDoNotUseRSSBeforeWrite);
INDEXLIB_UNIT_TEST_CASE(SliceFileNodeTest, TestSliceFileReaderNotSupport);
//////////////////////////////////////////////////////////////////////

SliceFileNodeTest::SliceFileNodeTest() {}

SliceFileNodeTest::~SliceFileNodeTest() {}

void SliceFileNodeTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH()).GetOrThrow();
}

void SliceFileNodeTest::CaseTearDown() {}

void SliceFileNodeTest::TestSliceFileReaderNotSupport()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    string fileContext = "hello";
    std::shared_ptr<SliceFileReader> fileReader =
        TestFileCreator::CreateSliceFileReader(fileSystem, FSST_MEM, fileContext);
    ASSERT_EQ(nullptr, fileReader->ReadToByteSliceList(fileContext.size(), 0, ReadOption()));
}

static int self_vmrss()
{
    char path_buf[64];
    char proc_pid_status_buf[1024];
    char vmrss_buf[32];
    char* vmrss;
    int i = 0;

    pid_t pid = getpid();
    sprintf(path_buf, "/proc/%d/status", pid);
    memset(proc_pid_status_buf, 0, 1024);

    int proc_pid_status_fd = open(path_buf, O_RDONLY);
    if (proc_pid_status_fd < 0) {
        return -1;
    }
    ssize_t ret = read(proc_pid_status_fd, proc_pid_status_buf, 1023);
    if (ret < 0) {
        return -1;
    }
    close(proc_pid_status_fd);

    vmrss = strstr(proc_pid_status_buf, "VmRSS:");
    if (!vmrss) {
        return -1;
    }
    vmrss += sizeof("VmRss:");
    memset(vmrss_buf, 0, 32);
    while (1) {
        if (*vmrss < 48 || *vmrss > 57) {
            vmrss++;
        } else {
            break;
        }
    }
    while (1) {
        if (*vmrss < 48 || *vmrss > 57) {
            break;
        } else {
            vmrss_buf[i] = *vmrss;
            vmrss++;
            i++;
        }
    }
    return atoi(vmrss_buf) * 1024;
}

void SliceFileNodeTest::TestDoNotUseRSSBeforeWrite()
{
    int rssStart = self_vmrss();

    uint64_t sliceLen = 1024 * 1024 * 1024;
    int32_t sliceNum = 1;
    SliceFileNode fileNode(sliceLen, sliceNum, _memoryController);

    int rssEnd1 = self_vmrss();
    // do not alloc rss before write
    ASSERT_TRUE((rssEnd1 - rssStart) < (int)sliceLen)
        << "rssEnd1-rssStart=" << rssEnd1 << "-" << rssStart << "=" << (rssEnd1 - rssStart) << ", "
        << "sliceLen=" << sliceLen;

    char buffer[1024] = {0};
    for (size_t i = 0; i < sliceLen / sizeof(buffer); ++i) {
        ASSERT_EQ(FSEC_OK, fileNode.Write(buffer, sizeof(buffer)).Code());
    }

    usleep(1000);
    int rssEnd2 = self_vmrss();

    cout << "rssEnd2-rssStart=" << rssEnd2 << "-" << rssStart << "=" << (rssEnd2 - rssStart) << ", "
         << "sliceLen=" << sliceLen << endl;

    ASSERT_TRUE((rssEnd2 - rssStart) >= (int)sliceLen)
        << "rssEnd2-rssStart=" << rssEnd2 << "-" << rssStart << "=" << (rssEnd2 - rssStart) << ", "
        << "sliceLen=" << sliceLen;
}
}} // namespace indexlib::file_system
