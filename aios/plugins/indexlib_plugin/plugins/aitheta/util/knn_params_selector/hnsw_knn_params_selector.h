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
#ifndef __INDEXLIB_AITHETA_UTIL_HNWS_KNN_PARAM_PARSER_H_
#define __INDEXLIB_AITHETA_UTIL_HNWS_KNN_PARAM_PARSER_H_

#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"

namespace indexlib {
namespace aitheta_plugin {

class HnswKnnParamsSelector : public GraphKnnParamsSelector {
 public:
    HnswKnnParamsSelector(const KnnStrategies &strategies) : GraphKnnParamsSelector(strategies) {}
    ~HnswKnnParamsSelector() = default;

    bool InitSearcherParams(const indexlib::util::KeyValueMap &parameters, aitheta::IndexParams &indexParams,
                            bool printParam) override;
    bool InitStreamerParams(const indexlib::util::KeyValueMap &parameters, aitheta::IndexParams &indexParams,
                            bool printParam) override;

    const std::string &GetSearcherName() override { return KNN_HNSW_SEARCHER; }
    const std::string &GetStreamerName() override { return KNN_HNSW_STREAMER; }

    int64_t GetOnlineSign(const aitheta::IndexParams &indexParams) override {
        std::string uniqTag = autil::StringUtil::toString(autil::TimeUtility::currentTimeInMicroSeconds());
        return IKnnParamsSelector::CreateSign(indexParams, uniqTag);
    }

 private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(HnswKnnParamsSelector);

}
}
#endif  // __INDEXLIB_AITHETA_UTIL_HNWS_KNN_PARAM_PARSER_H_
