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

#ifndef __INDEXLIB_AITHETA_UTIL_KNN_SEARCHER_FACTORY_H_
#define __INDEXLIB_AITHETA_UTIL_KNN_SEARCHER_FACTORY_H_

#include "indexlib/common_define.h"

#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index/index_attr.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/lr_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/pq_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/hc_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"

namespace indexlib {
namespace aitheta_plugin {

class KnnSearcherFactory {
 public:
    KnnSearcherFactory();
    ~KnnSearcherFactory();

 public:
    static IndexSearcherPtr Create(const SchemaParameter &schemaParameter, const KnnConfig &knnCfg,
                                   const IndexAttr &indexAttr, bool isBuiltOnline, IndexParams &searchParams,
                                   int64_t &signature, bool &enableGpu);

 private:
    IE_LOG_DECLARE();
};

}
}

#endif  // __INDEXLIB_AITHETA_UTIL_KNN_SEARCHER_FACTORY_H_
