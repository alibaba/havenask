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
 * @file   knn_params_selector.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 16:47:43 2019
 *
 * @brief
 *
 *
 */

#ifndef __INDEXLIB_AITHETA_UTIL_HC_KNN_PARAM_PARSER_H_
#define __INDEXLIB_AITHETA_UTIL_HC_KNN_PARAM_PARSER_H_
#include <aitheta/index_framework.h>
#include "indexlib/common_define.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/misc/log.h"
#include "indexlib/indexlib.h"
#include <proxima/common/params_define.h>

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/i_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"

namespace indexlib {
namespace aitheta_plugin {

class HCKnnParamsSelector : public NoneLinearKnnParamsSelector {
 public:
    HCKnnParamsSelector(const KnnStrategies &strategies);
    virtual ~HCKnnParamsSelector();

    virtual bool InitBuilderParams(const indexlib::util::KeyValueMap &parameters, aitheta::IndexParams &indexParams,
                                   bool printParam = true) override;

    virtual bool InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                    aitheta::IndexParams &indexParams, bool printParam = true) override;
    virtual bool InitStreamerParams(const indexlib::util::KeyValueMap &parameters,
                                    aitheta::IndexParams &indexParams, bool printParam = true) override {
        return false;
    }

    const std::string &GetBuilderName() override { return KNN_HC_BUILDER; }
    const std::string &GetSearcherName() override { return mSearcherName; }
    const std::string &GetStreamerName() override { return INVALID_HC_STREAMER; }

 private:
    std::string mSearcherName;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(HCKnnParamsSelector);

}
}
#endif  // __INDEXLIB_AITHETA_UTIL_HC_KNN_PARAM_PARSER_H_
