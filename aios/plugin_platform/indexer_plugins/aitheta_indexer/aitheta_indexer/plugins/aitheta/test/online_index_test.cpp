#include "aitheta_indexer/plugins/aitheta/test/online_index_test.h"

#include <fstream>
#include <proxima/common/params_define.h>
#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer_define.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/test/schema_maker.h>

#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void OnlineIndexTest::TestHC() {
    util::KeyValueMap params = {
        {DIMENSION, "128"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, L2}};
    InnerTestIndex(params);
}

void OnlineIndexTest::InnerTestIndex(const util::KeyValueMap& params) {
    return;
    IndexerPtr indexer(mFactory->createIndexer(params));
    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
    IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));

    ASSERT_TRUE(nullptr != indexRetriever);
    ASSERT_TRUE(nullptr != indexSegRetriever);

    string indexPath = string(TEST_DATA_PATH) + "ops_aitheta_recall_0/";
    auto indexDir = IndexlibIoWrapper::CreateDirectory(indexPath);
    ASSERT_NE(nullptr, indexDir);

    ASSERT_TRUE(indexSegRetriever->Open(indexDir));

    std::vector<IndexSegmentRetrieverPtr> retrievers;
    retrievers.push_back(indexSegRetriever);
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
    std::vector<docid_t> baseDocIds;
    baseDocIds.push_back(0);
    indexRetriever->Init(deletionMapReader, retrievers, baseDocIds);

    autil::mem_pool::Pool pool;
    std::string q;
    std::string query;
    std::string topK;
    common::Term term;
    vector<SegmentMatchInfo> segMatchInfos;
    MatchInfoPtr matchInfo;
    float score = 0.0F;

    q = "-0.027210,0.004770,-0.073550,0.018302,-0.080792,-0.147644,-0.000473,0.048447,-0.091430,-0.001567,0.044012,-0."
        "115647,-0.161364,-0.082717,-0.155263,0.013741,-0.176663,-0.061614,0.005987,0.063602,-0.053084,0.002707,-0."
        "094170,0.018623,0.078429,-0.137591,0.006239,-0.155870,-0.045441,0.061170,0.029535,0.001532,0.052225,0.100201,"
        "0.037734,0.006639,-0.053198,-0.171778,0.044010,-0.071591,-0.011711,0.015718,0.059592,-0.065333,0.076869,-0."
        "013033,0.033645,0.016493,0.001544,0.004077,-0.011588,-0.002426,0.075730,0.042767,-0.067184,-0.158561,-0."
        "012670,0.024659,-0.068708,0.010118,0.079444,0.008233,-0.174261,0.077496,-0.166102,0.020735,-0.170266,-0."
        "169444,-0.091034,-0.115181,0.085434,-0.002130,0.004650,0.094216,0.050918,-0.161460,0.157608,-0.114888,0."
        "052764,0.074202,-0.070774,0.106408,-0.005383,0.029183,-0.171382,0.170559,0.005016,-0.000693,-0.140164,-0."
        "113981,0.010016,-0.143296,0.027272,-0.109879,0.001404,0.023241,0.096610,0.031643,-0.178830,-0.057622,0.016626,"
        "-0.161813,-0.153554,0.010782,-0.165066,-0.040387,-0.168625,0.008460,-0.003133,0.070717,-0.162667,0.051966,0."
        "024135,0.020409,-0.000026,-0.041652,-0.085045,0.004433,0.079026,0.039800,0.025248,-0.153041,-0.115419,0."
        "085151,-0.034460,-0.165283,0.000269,0.024541";

    topK = "50";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;

    std::map<float, docid_t> score2docid;
    for (size_t i = 0; i < matchInfo->matchCount; ++i) {
        float score = 0.0F;
        memcpy(&score, matchInfo->matchValues + i, sizeof(score));
        score2docid[score] = matchInfo->docIds[i];
    }
    for (const auto& kv : score2docid) {
        cout << 1 - kv.first/2 << " " << kv.second << endl;
    }
}

IE_NAMESPACE_END(aitheta_plugin);
