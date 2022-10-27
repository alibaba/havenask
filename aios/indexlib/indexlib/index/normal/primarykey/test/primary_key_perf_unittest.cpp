#include "indexlib/test/schema_maker.h"
#include "indexlib/index/normal/primarykey/test/primary_key_perf_unittest.h"
#include "indexlib/index/normal/primarykey/test/primary_key_test_case_helper.h"
#include "indexlib/index/normal/primarykey/primary_key_index_writer_typed.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/util/simple_pool.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, PrimaryKeyPerfTest);

PrimaryKeyPerfTest::PrimaryKeyPerfTest()
{
}

PrimaryKeyPerfTest::~PrimaryKeyPerfTest()
{
}

void PrimaryKeyPerfTest::CaseSetUp()
{
    mRootDir = GET_SEGMENT_DIRECTORY();
    mPool.reset(new Pool());
}

void PrimaryKeyPerfTest::CaseTearDown()
{
}

void PrimaryKeyPerfTest::TestDumpPerf()
{
    IndexConfigPtr pkConfig = PrimaryKeyTestCaseHelper::CreateIndexConfig<uint64_t>(
            "pk", pk_hash_table, false);
    DoTestDumpPerf(pkConfig);
    pkConfig = PrimaryKeyTestCaseHelper::CreateIndexConfig<uint64_t>(
            "pk", pk_sort_array, false);
    DoTestDumpPerf(pkConfig);
}

void PrimaryKeyPerfTest::DoTestDumpPerf(const IndexConfigPtr& pkConfig)
{
    TearDown();
    SetUp();
    UInt64PrimaryKeyIndexWriterTypedPtr pkWriter(
            new UInt64PrimaryKeyIndexWriterTyped());
    pkWriter->Init(pkConfig, NULL);

    size_t docCount = 1024*1024*10;
    IndexDocument indexDocument(mPool.get());
    for (size_t i = 0; i < docCount; ++i)
    {
        indexDocument.SetDocId(i);
        string pkStr = autil::StringUtil::toString(i);
        indexDocument.SetPrimaryKey(pkStr);
        pkWriter->EndDocument(indexDocument);
    }
    SimplePool simplePool;
    int64_t beginTime = TimeUtility::currentTime();
    pkWriter->Dump(mRootDir, &simplePool);
    int64_t endTime = TimeUtility::currentTime();
    IE_LOG(ERROR, "time %ld ms", (endTime - beginTime) / 1000);
    pkWriter.reset();
}


void PrimaryKeyPerfTest::TestLoadPerf()
{

}

IE_NAMESPACE_END(index);

