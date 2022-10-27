#include "indexlib/misc/test/error_log_collector_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <fslib/fs/FileSystem.h>
#include <fslib/fs/File.h>
#include <cstdio>
using namespace std;
using namespace autil;
using namespace fslib;

using namespace fslib::fs;
IE_NAMESPACE_BEGIN(misc);
IE_NAMESPACE_USE(storage);

IE_LOG_SETUP(misc, ErrorLogCollectorTest);

ErrorLogCollectorTest::ErrorLogCollectorTest()
{
}

ErrorLogCollectorTest::~ErrorLogCollectorTest()
{
}

void ErrorLogCollectorTest::CaseSetUp()
{
    // if (FileSystemWrapper::IsExist("error_log_collector.log"))
    // {
    //     FileSystemWrapper::DeleteFile("error_log_collector.log");
    //     cout << "delete log" << endl;
    //     auto p = FileSystemWrapper::OpenFile("error_log_collector.log", WRITE, true);
    //     p->Close();
    // }
    setenv("COLLECT_ERROR_LOG", "true", 1);
}

void ErrorLogCollectorTest::CaseTearDown()
{
}

void ErrorLogCollectorTest::TestSimpleProcess()
{
    DECLARE_ERROR_COLLECTOR_IDENTIFIER("abcde");
    ASSERT_TRUE(ErrorLogCollector::UsingErrorLogCollect());
    for(size_t i = 0; i < 64; i ++)
    {
        ERROR_COLLECTOR_LOG(ERROR, "test [%s]", "error");
    }
    std::string content;
    FileSystemWrapper::AtomicLoad("error_log_collector.log", content);
    for(size_t i = 0; i < 6; i ++)
    {
        ASSERT_TRUE(strstr(content.c_str(),StringUtil::toString(1 << i).c_str()));
    }
    FileSystemWrapper::DeleteFile("error_log_collector.log");
}

IE_NAMESPACE_END(misc);

