#include "indexlib/index/normal/inverted_index/builtin_index/range/test/range_index_reader_unittest.h"

#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/searcher.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, RangeIndexReaderTest);

RangeIndexReaderTest::RangeIndexReaderTest() {}

RangeIndexReaderTest::~RangeIndexReaderTest() {}

void RangeIndexReaderTest::CaseSetUp()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRootDir = GET_TEMP_DATA_PATH();

    mCacheLoadConfig = R"(
        {
            "load_config":[
                {
                    "file_patterns":[
                        "index/price/(price_@_bottom|price_@_high)/dictionary"
                    ],
                    "load_strategy":"cache",
                    "load_strategy_param":{
                        "direct_io":false,
                        "global_cache":true
                    }
                }
            ]
        }
    )";

    mMMapLockLoadConfig = R"(
        {
            "load_config":[
                {
                    "file_patterns":[
                        "index/price/(price_@_bottom|price_@_high)/dictionary"
                    ],
                    "load_strategy":"mmap",
                    "load_strategy_param":{
                        "lock":true
                    }
                }
            ]
        }
    )";

    mMMapNoLockLoadConfig = R"(
        {
            "load_config":[
                {
                    "file_patterns":[
                        "index/price/(price_@_bottom|price_@_high)/dictionary"
                    ],
                    "load_strategy":"mmap",
                    "load_strategy_param":{
                        "lock":false
                    }
                }
            ]
        }
    )";
}

void RangeIndexReaderTest::CaseTearDown() {}

void RangeIndexReaderTest::TestRangeIndexReaderTermIllegal()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[100, 101]", "docid=0,pk=a;"));
    IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetInvertedIndexReader();
    Term term("128", "price");
    PostingIterator* iter = indexReader->Lookup(term).ValueOrThrow();
    ASSERT_FALSE(iter);
}

void RangeIndexReaderTest::TestMultiValueDocNumber()
{
    string field = "price:int64:true;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDocString = "cmd=add,price=100 201,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000 1522,pk=b,ts=1";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[200, 201]", "docid=0,pk=a;"));
    partition::IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetInvertedIndexReader();
    Int64Term term(200, true, 201, true, "price");
    PostingIterator* iter = indexReader->Lookup(term).ValueOrThrow();
    SeekAndFilterIterator* stIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(stIter);
    DocValueFilter* dvIter = stIter->GetDocValueFilter();
    ASSERT_TRUE(dvIter->Test(0));
    ASSERT_FALSE(dvIter->Test(1));
    delete iter;
}

void RangeIndexReaderTest::InnerTestSimple(const string& loadConfig, bool useBlockCache)
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    FromJsonString(options.GetOnlineConfig().loadConfigList, loadConfig);
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[100, 101]", "docid=0,pk=a;"));
    auto blockCache =
        psm.GetIndexPartitionResource().fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache();
    ASSERT_TRUE(blockCache);
    if (useBlockCache) {
        ASSERT_EQ(2, blockCache->GetTotalMissCount());
    } else {
        ASSERT_EQ(0, blockCache->GetTotalMissCount());
    }
    partition::IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetInvertedIndexReader();
    Int64Term term(100, true, 101, true, "price");
    PostingIterator* iter = indexReader->Lookup(term).ValueOrThrow();
    SeekAndFilterIterator* stIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(stIter);
    DocValueFilter* dvIter = stIter->GetDocValueFilter();
    ASSERT_TRUE(dvIter->Test(0));
    ASSERT_FALSE(dvIter->Test(1));
    delete iter;

    iter = indexReader->PartialLookup(term, {}, 1000, pt_default, nullptr).ValueOrThrow();
    stIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(stIter);
    dvIter = stIter->GetDocValueFilter();
    ASSERT_TRUE(dvIter->Test(0));
    ASSERT_FALSE(dvIter->Test(1));
    delete iter;
}

void RangeIndexReaderTest::PreparePSM(const string& loadConfig, const string& loadName, const DocVec& docs,
                                      bool enableFileCompress, PartitionStateMachine* psm)
{
    stringstream ss;
    for (const auto& doc : docs) {
        ss << "cmd=add, pk=" << doc.first << ",price=" << doc.second << ";";
    }
    string fullDocString = ss.str();
    IndexPartitionOptions options;
    auto schema = mSchema;
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "price:compressor";
        string attrCompressStr = "price:compressor";
        schema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }
    FromJsonString(options.GetOnlineConfig().loadConfigList, loadConfig);
    string root = mRootDir + "/" + loadName + (enableFileCompress ? "_compress" : "");
    ASSERT_TRUE(psm->Init(schema, options, root));
    ASSERT_TRUE(psm->Transfer(BUILD_FULL, fullDocString, "", ""));
}

void RangeIndexReaderTest::TestMMapNoLock() { InnerTestSimple(mMMapNoLockLoadConfig, false); }

void RangeIndexReaderTest::TestMMapLock() { InnerTestSimple(mMMapLockLoadConfig, false); }

void RangeIndexReaderTest::TestCache() { InnerTestSimple(mCacheLoadConfig, true); }

void RangeIndexReaderTest::InnerTestRead(const string& loadConfig, const string& loadName, const DocVec& docs,
                                         const QueryVec& querys)
{
    bool enableFileCompress = GET_CASE_PARAM();
    IE_LOG(INFO, "case loadName[%s], enableFileCompress[%d] begin", loadName.c_str(), enableFileCompress);
    PartitionStateMachine psm;
    PreparePSM(loadConfig, loadName, docs, enableFileCompress, &psm);
    auto blockCache =
        psm.GetIndexPartitionResource().fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache();
    for (auto& query : querys) {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "price:" + query.first, query.second))
            << query.first << " " << query.second;
    }

    if (loadName == "block") {
        ASSERT_LT(0, blockCache->GetTotalMissCount());
    } else {
        ASSERT_EQ(0, blockCache->GetTotalMissCount());
    }
}

void RangeIndexReaderTest::TestFewItems()
{
    InnerTestRead(mMMapLockLoadConfig, "mmap_lock", {{"1", "1"}}, {{"[1, 1]", "pk=1"}});
    InnerTestRead(mCacheLoadConfig, "block", {{"1", "1"}}, {{"[1, 1]", "pk=1"}});
}

void RangeIndexReaderTest::InnerTestManyItems(const std::string& loadConfig, const std::string& loadName)
{
    const size_t VALUE_LIMIT = 1024 * 32;
    DocVec docs;
    // {pk, price} : {0, 0}, {2, 2} {4, 4} ...
    for (size_t i = 0; i < VALUE_LIMIT; i += 2) {
        docs.emplace_back(to_string(i), to_string(i));
    }
    QueryVec querys;
    // point query
    for (size_t i = 0; i < VALUE_LIMIT; i += 2) {
        querys.push_back({"[" + to_string(i) + ", " + to_string(i) + "]", "pk=" + to_string(i)});
        querys.push_back({"[" + to_string(i + 1) + ", " + to_string(i + 1) + "]", ""});
    }
    reverse(querys.begin(), querys.end());
    // range query
    querys.push_back({"[0, 1]", "pk=0"});
    querys.push_back({"[1, 3]", "pk=2"});
    querys.push_back({"[125, 131]", "pk=126;pk=128;pk=130"});
    querys.push_back({"[127, 129]", "pk=128"});

    // [end-5, end+2] => {end-4, end-2}
    querys.push_back({"[" + to_string(VALUE_LIMIT - 5) + ", " + to_string(VALUE_LIMIT + 2) + "]",
                      "pk=" + to_string(VALUE_LIMIT - 4) + ";" + "pk=" + to_string(VALUE_LIMIT - 2)});

    // [end, end] => {}
    querys.push_back({"[" + to_string(VALUE_LIMIT) + "," + to_string(VALUE_LIMIT) + "]", ""});

    // test compress and uncompress
    InnerTestRead(loadConfig, loadName, docs, querys);
}

void RangeIndexReaderTest::TestManyItemsMMapLock() { InnerTestManyItems(mMMapLockLoadConfig, "mmap_lock"); }

void RangeIndexReaderTest::TestManyItemsMMapNoLock() { InnerTestManyItems(mMMapNoLockLoadConfig, "mmap_nolock"); }

void RangeIndexReaderTest::TestManyItemsCache() { InnerTestManyItems(mCacheLoadConfig, "block"); }

void RangeIndexReaderTest::InnerTestConcurrencyQuery(const string& loadConfig, const string& loadName)
{
    const size_t VALUE_LIMIT = 1024 * 32;
    DocVec docs;
    // {pk, price} : {0, 0}, {2, 2} {4, 4} ...
    for (size_t i = 0; i < VALUE_LIMIT; i += 2) {
        docs.emplace_back(to_string(i), to_string(i));
    }
    PartitionStateMachine psm;
    bool enableFileCompress = GET_CASE_PARAM();
    PreparePSM(loadConfig, loadName, docs, enableFileCompress, &psm);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:[1,6]", "pk=2;pk=4;pk=6"));

    vector<autil::ThreadPtr> threads;
    auto reader = psm.GetIndexPartition()->GetReader()->GetInvertedIndexReader("price");
    for (size_t i = 0; i < 10; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread([&]() {
            for (int j = 0; j < 1024; ++j) {
                // use psm interface to test
                ASSERT_TRUE(psm.Transfer(QUERY, "", "price:[1,6]", "pk=2;pk=4;pk=6"));
                ASSERT_TRUE(psm.Transfer(QUERY, "", "price:[1001,1006]", "pk=1002;pk=1004;pk=1006"));
                ASSERT_TRUE(psm.Transfer(QUERY, "", "price:[202,202]", "pk=202"));
                ASSERT_TRUE(psm.Transfer(QUERY, "", "price:[103,103]", ""));
                ASSERT_TRUE(
                    psm.Transfer(QUERY, "", "price:[1,20]", "pk=2;pk=4;pk=6;pk=8;pk=10;pk=12;pk=14;pk=16;pk=18;pk=20"));
                ASSERT_TRUE(psm.Transfer(QUERY, "", "price:[780,783]", "pk=780;pk=782"));

                // use native interface to test
                std::shared_ptr<PostingIterator> iter(
                    reader->Lookup(Int64Term(1, true, 6, true, "price")).ValueOrThrow());
                ASSERT_TRUE(iter);
                docid_t docId = 0;
                vector<docid_t> ans;
                while ((docId = iter->SeekDoc(docId)) != INVALID_DOCID) {
                    ans.push_back(docId);
                }
                vector<docid_t> expected {1, 2, 3};
                ASSERT_EQ(expected, ans);
            }
        });
        threads.emplace_back(thread);
    }
    for (auto& thread : threads) {
        thread->join();
    }
}

void RangeIndexReaderTest::TestConcurrencyQuery()
{
    InnerTestConcurrencyQuery(mCacheLoadConfig, "block");
    InnerTestConcurrencyQuery(mMMapNoLockLoadConfig, "mmap_nolock");
}

}} // namespace indexlib::index
