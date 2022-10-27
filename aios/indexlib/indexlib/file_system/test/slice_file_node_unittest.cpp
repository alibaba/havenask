#include <sstream>
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/slice_file_node_unittest.h"
#include "indexlib/file_system/slice_file_node.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SliceFileNodeTest);

SliceFileNodeTest::SliceFileNodeTest()
{
}

SliceFileNodeTest::~SliceFileNodeTest()
{
}

void SliceFileNodeTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void SliceFileNodeTest::CaseTearDown()
{
}

void SliceFileNodeTest::TestSliceFileReaderNotSupport()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    string fileContext = "hello";
    FileReaderPtr fileReader = TestFileCreator::CreateSliceFileReader(
            fileSystem, FSST_IN_MEM, fileContext);

    ASSERT_THROW(fileReader->Read(fileContext.size(), 0),
                 UnSupportedException);
}

static int self_vmrss()
{
    char path_buf[64];
    char proc_pid_status_buf[1024];
    char vmrss_buf[32];
    char *vmrss;
    int i = 0;
    
    pid_t pid = getpid();
    sprintf(path_buf, "/proc/%d/status", pid);
    memset(proc_pid_status_buf, 0, 1024);
    
    int proc_pid_status_fd = open(path_buf, O_RDONLY);
    if(proc_pid_status_fd < 0){ 
        return -1;
    }
    read(proc_pid_status_fd, proc_pid_status_buf, 1023);
    close(proc_pid_status_fd);
    
    vmrss = strstr(proc_pid_status_buf, "VmRSS:");
    if(!vmrss){
        return -1;
    }
    vmrss += sizeof("VmRss:");
    memset(vmrss_buf, 0, 32);
    while(1){
        if(*vmrss < 48 || *vmrss > 57){
            vmrss++;
        }else{
            break;
        }
    }
    while(1){
        if(*vmrss < 48 || *vmrss > 57){
            break;
        }else{
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
    SliceFileNode fileNode(sliceLen, sliceNum, mMemoryController);
    
    int rssEnd1 = self_vmrss();
    // do not alloc rss before write
    ASSERT_TRUE((rssEnd1 - rssStart) < (int)sliceLen)
        << "rssEnd1-rssStart="
        << rssEnd1 << "-" << rssStart << "=" 
        << (rssEnd1 - rssStart) << ", "
        << "sliceLen=" << sliceLen;
    
    char buffer[1024] = { 0 };
    for (size_t i = 0; i < sliceLen / sizeof(buffer); ++i)
    {
        fileNode.Write(buffer, sizeof(buffer));
    }

    usleep(1000);
    int rssEnd2 = self_vmrss();
    
    cout << "rssEnd2-rssStart="
         << rssEnd2 << "-" << rssStart << "=" 
         << (rssEnd2 - rssStart) << ", "
         << "sliceLen=" << sliceLen << endl;

    ASSERT_TRUE((rssEnd2 - rssStart) >= (int)sliceLen)
        << "rssEnd2-rssStart="
        << rssEnd2 << "-" << rssStart << "=" 
        << (rssEnd2 - rssStart) << ", "
        << "sliceLen=" << sliceLen;
}

IE_NAMESPACE_END(file_system);

