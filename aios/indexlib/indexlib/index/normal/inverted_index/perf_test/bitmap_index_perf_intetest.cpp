#include "indexlib/index/normal/inverted_index/perf_test/bitmap_index_perf_intetest.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapIndexPerfTest);

BitmapIndexPerfTest::BitmapIndexPerfTest()
{
}

BitmapIndexPerfTest::~BitmapIndexPerfTest()
{
}

void BitmapIndexPerfTest::CaseSetUp()
{
    string field = "string1:string;";
    string index = "index1:string:string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("dict", "hello");
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    indexConfig->SetDictConfig(dictConfig);

    IndexPartitionOptions options;
    mPsm.reset(new PartitionStateMachine);
    ASSERT_TRUE(mPsm->Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(mPsm->Transfer(BUILD_RT, "cmd=add,string1=hello,ts=0;", "", ""));
}

void BitmapIndexPerfTest::CaseTearDown()
{
}

void BitmapIndexPerfTest::DoWrite()
{
    size_t counter = 0;
    while (!IsFinished())
    {
        ASSERT_TRUE(mPsm->Transfer(BUILD_RT, "cmd=add,string1=hello,ts=1;", "", ""));
        counter++;
        if (counter % 10000 == 0)
        {
            cout << "counter:" << counter << endl;
        }
    }
}

void BitmapIndexPerfTest::DoRead(int* errCode)
{
    while (!IsFinished())
    {
        mPsm->Transfer(QUERY, "", "index1:hello", "");
    }
}

void BitmapIndexPerfTest::TestSimpleProcess()
{
    DoMultiThreadTest(10, 5);
}

IE_NAMESPACE_END(index);

