/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @file   knn_params_selector_factory.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 15:24:02 2019
 *
 * @brief
 *
 *
 */

#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/lr_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/pq_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/hc_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/hnsw_knn_params_selector.h"

using namespace std;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, KnnParamsSelectorFactory);

IKnnParamsSelectorPtr KnnParamsSelectorFactory::CreateSelector(const string &knnName, const SchemaParameter &schemaParameter,
                                                               const KnnConfig &knnCfg, size_t docNum, bool isBuiltOnline) {
    if (isBuiltOnline) {
        KnnStrategies knnStrategies;
        auto iterator = knnCfg.type2Strategies.find(INDEX_TYPE_HNSW);
        if (knnCfg.type2Strategies.end() != iterator) {
            knnStrategies = iterator->second;
        }
        return IKnnParamsSelectorPtr(new HnswKnnParamsSelector(knnStrategies));
    }

    string name = knnName;
    if (IKnnParamsSelector::EnableKnnLr(schemaParameter, docNum)) {
        name = INDEX_TYPE_LR;
    }

    KnnStrategies knnStrategies;
    auto iterator = knnCfg.type2Strategies.find(name);
    if (knnCfg.type2Strategies.end() != iterator) {
        knnStrategies = iterator->second;
    }

    if (name == INDEX_TYPE_LR) {
        return IKnnParamsSelectorPtr(new LRKnnParamsSelector(knnStrategies));
    }

    if (name == INDEX_TYPE_PQ) {
        return IKnnParamsSelectorPtr(new PQKnnParamsSelector(knnStrategies));
    }
    if (name == INDEX_TYPE_HC) {
        return IKnnParamsSelectorPtr(new HCKnnParamsSelector(knnStrategies));
    }
    if (name == INDEX_TYPE_GRAPH) {
        return IKnnParamsSelectorPtr(new GraphKnnParamsSelector(knnStrategies));
    }
    if (name == INDEX_TYPE_HNSW) {
        return IKnnParamsSelectorPtr(new HnswKnnParamsSelector(knnStrategies));
    }
    return IKnnParamsSelectorPtr();
}


}
}
