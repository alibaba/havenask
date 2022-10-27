#include "indexlib/common/numeric_compress/perf_test/equivalent_compress_perf_unittest.h"
#include <stdlib.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

#define READ_TIMES 100000000

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, EquivalentCompressPerfTest);

EquivalentCompressPerfTest::EquivalentCompressPerfTest()
{

}

EquivalentCompressPerfTest::~EquivalentCompressPerfTest()
{
}

void EquivalentCompressPerfTest::SetUp()
{
    string dataPath = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "section_offset_testdata.tgz");
    string cmd = "tar -zxvf " + dataPath + " -C " + TEST_DATA_PATH;
    system(cmd.c_str());
}

void EquivalentCompressPerfTest::TearDown()
{
    string dataPath = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "section_offset_testdata");
    if (FileSystemWrapper::IsExist(dataPath))
    {
        FileSystemWrapper::DeleteDir(dataPath);
    }
}

void EquivalentCompressPerfTest::TestSimpleProcess()
{
    {
        string dataPath = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "section_offset_testdata/offset_30MB");
        InnerTestRead(dataPath, READ_TIMES, 10000);
        InnerTestRead(dataPath, READ_TIMES, 100);
        InnerTestRead(dataPath, READ_TIMES, 10);
        InnerTestRead(dataPath, READ_TIMES, 1);
    }

    {
        string dataPath = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "section_offset_testdata/offset_7MB");
        InnerTestRead(dataPath, READ_TIMES, 10000);
        InnerTestRead(dataPath, READ_TIMES, 100);
        InnerTestRead(dataPath, READ_TIMES, 10);
        InnerTestRead(dataPath, READ_TIMES, 1);
    }

    {
        string dataPath = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "section_offset_testdata/offset_600KB");
        InnerTestRead(dataPath, READ_TIMES, 10000);
        InnerTestRead(dataPath, READ_TIMES, 100);
        InnerTestRead(dataPath, READ_TIMES, 10);
        InnerTestRead(dataPath, READ_TIMES, 1);
    }
    
}

void EquivalentCompressPerfTest::InnerTestRead(
        string& filePath, uint32_t readTimes, uint32_t step)
{
    file_system::InMemFileNodePtr dataFile(file_system::InMemFileNodeCreator::Create());
    dataFile->Open(filePath, file_system::FSOT_IN_MEM);
    dataFile->Populate();
    EquivalentCompressReader<uint32_t> reader((uint8_t*)dataFile->GetBaseAddress());
    uint32_t itemCount = (size_t)reader.Size();

    vector<uint32_t> array;
    array.reserve(itemCount);
    for (uint32_t i = 0; i < itemCount; i++)
    {
        array.push_back(reader[i]);
    }

    uint32_t firstItem = rand() % itemCount;
    uint32_t currentItem = firstItem;
    uint32_t result;
    int64_t beginTime = TimeUtility::currentTime();
    for (uint32_t i = 0; i < READ_TIMES; ++i)
    {
        result = array[currentItem];
        currentItem = (currentItem + step) % itemCount;
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "##########" << result <<"##########" << endl;
    cout << "normal array search data:" << filePath << " times:"
         << readTimes << " inc step:" << step
         << " use time: " << (endTime - beginTime)/1000
         << "ms." << endl;


    currentItem = firstItem;
    beginTime = TimeUtility::currentTime();
    for (uint32_t i = 0; i < READ_TIMES; ++i)
    {
        result = reader.Get(currentItem);
        currentItem = (currentItem + step) % itemCount;
    }
    endTime = TimeUtility::currentTime();
    cout << "search data:" << filePath << " times:"
         << readTimes << " inc step:" << step
         << " use time: " << (endTime - beginTime)/1000
         << "ms." << endl;
    cout << "##########" << result <<"##########" << endl;
    dataFile->Close();
}

void EquivalentCompressPerfTest::TestCalculateCompressLen()
{
    InnerTestCalculateCompressLen<uint32_t>(30000000);
    InnerTestCalculateCompressLen<uint64_t>(30000000);

    InnerTestCalculateCompressLen<uint32_t>(2000000);
    InnerTestCalculateCompressLen<uint64_t>(2000000);
}

IE_NAMESPACE_END(common);

