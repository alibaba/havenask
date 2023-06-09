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
 * @file   knn_params_selector_factory.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 15:24:09 2019
 *
 * @brief
 *
 *
 */
#ifndef __INDEXLIB_AITHETA_UTIL_KNN_PARAM_PARSER_FACTORY_H_
#define __INDEXLIB_AITHETA_UTIL_KNN_PARAM_PARSER_FACTORY_H_

#include <map>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/i_knn_params_selector.h"

namespace indexlib {
namespace aitheta_plugin {

class KnnParamsSelectorFactory {
 public:
    KnnParamsSelectorFactory() = default;
    ~KnnParamsSelectorFactory() = default;

 public:
    static IKnnParamsSelectorPtr CreateSelector(const std::string &knnName, const SchemaParameter &schemaParameter,
                                                const KnnConfig &knnCfg, size_t docNum, bool isBuiltOnline = false);

 private:
    IE_LOG_DECLARE();
};

}
}

#endif  // __INDEXLIB_AITHETA_UTIL_KNN_PARAM_PARSER_FACTORY_H_
