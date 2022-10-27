#include "indexlib/index/normal/inverted_index/test/index_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexReaderTest);

IndexReaderTest::IndexReaderTest()
{
}

IndexReaderTest::~IndexReaderTest()
{
}

void IndexReaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "pk:uint64:pk;string1:string;string2:string";
    string index = "pk:primarykey64:pk;index1:string:string1;index2:string:string2";
    string attr = "string1;string2";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    
    mOptions = IndexPartitionOptions();
}

void IndexReaderTest::CaseTearDown()
{
}

void IndexReaderTest::TestSimpleProcess()
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,pk=0,string1=0,string2=1;"
                       "cmd=add,pk=10,string1=10,string2=11;"
                       "cmd=add,pk=20,string1=20,string2=21;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:0", "docid=0,string1=0,string2=1"));
    auto indexReader = psm.GetIndexPartition()->GetReader()->GetIndexReader();
    ASSERT_TRUE(indexReader);

    std::vector<IndexReader::LookupContext> contexts;
    std::vector<std::pair<std::string, std::string>> termInfos
        = { { "index1", "0" }, { "index1", "10" }, { "index2", "21" } };

    for (const auto& termInfo : termInfos)
    {
        IndexReader::LookupContext context;
        context.term = new Term(termInfo.second, termInfo.first);
        contexts.push_back(context);
    }
    auto postingIters = indexReader->BatchLookup(contexts, nullptr);
    ASSERT_EQ(postingIters.size(), contexts.size());

    auto checkPostingIter = [this](PostingIterator* iter, std::vector<docid_t> docIds) mutable {
        ASSERT_TRUE(iter);
        for (auto docId : docIds)
        {
            ASSERT_EQ(docId, iter->SeekDoc(docId));
        }
        delete iter;
    };
    checkPostingIter(postingIters[0], {0});
    checkPostingIter(postingIters[1], {1});
    checkPostingIter(postingIters[2], {2});    

    for (auto& context : contexts)
    {
        delete context.term;
    }
}

IE_NAMESPACE_END(index);

