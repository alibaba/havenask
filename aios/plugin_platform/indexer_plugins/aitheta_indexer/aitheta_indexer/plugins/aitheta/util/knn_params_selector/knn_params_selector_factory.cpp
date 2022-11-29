/**
 * @file   knn_params_selector_factory.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 15:24:02 2019
 *
 * @brief
 *
 *
 */

#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/lr_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/pq_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/hc_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"

using namespace std;
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, KnnParamsSelectorFactory);

KnnParamsSelectorFactory::KnnParamsSelectorFactory() {}

KnnParamsSelectorFactory::~KnnParamsSelectorFactory() {}

IKnnParamsSelectorPtr KnnParamsSelectorFactory::CreateSelector(const string &knnName, const CommonParam &commonParam,
                                                               const KnnConfig &knnDynamicConfigs, size_t docSize) {
    string name = knnName;
    if (KnnParamsSelectorFactory::EnableKnnLr(commonParam, docSize)) {
        name = INDEX_TYPE_LR;
    }

    KnnStrategies knnStrategies;
    auto itr = knnDynamicConfigs.type2Strategies.find(name);
    if (knnDynamicConfigs.type2Strategies.end() != itr) {
        knnStrategies = itr->second;
    }
    if (name == INDEX_TYPE_LR) {
        return IKnnParamsSelectorPtr(new LRKnnParamsSelector(knnStrategies));
    } else if (name == INDEX_TYPE_PQ) {
        return IKnnParamsSelectorPtr(new PQKnnParamsSelector(knnStrategies));
    } else if (name == INDEX_TYPE_HC) {
        return IKnnParamsSelectorPtr(new HCKnnParamsSelector(knnStrategies));
    } else if (name == INDEX_TYPE_GRAPH) {
        return IKnnParamsSelectorPtr(new GraphKnnParamsSelector(knnStrategies));
    } else {
        return IKnnParamsSelectorPtr();
    }
}

bool KnnParamsSelectorFactory::EnableKnnLr(const CommonParam &commonParam, size_t docSize) {
    return docSize <= commonParam.mKnnLrThold;
}

IE_NAMESPACE_END(aitheta_plugin);
