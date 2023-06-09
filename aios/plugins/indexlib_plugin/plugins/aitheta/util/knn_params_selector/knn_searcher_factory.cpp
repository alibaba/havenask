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
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_searcher_factory.h"

using namespace std;
using namespace aitheta;
using namespace autil;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, KnnSearcherFactory);

IndexSearcherPtr KnnSearcherFactory::Create(const SchemaParameter &schemaParameter, const KnnConfig &knnCfg,
                                            const IndexAttr &indexAttr, bool isBuiltOnline, IndexParams &indexParams,
                                            int64_t &signature, bool &enableGpu) {
    string indexType = schemaParameter.indexType;
    uint32_t docNum = indexAttr.docNum;

    auto selector = KnnParamsSelectorFactory::CreateSelector(indexType, schemaParameter, knnCfg, docNum, isBuiltOnline);
    if (!selector) {
        IE_LOG(ERROR, "failed to get [%s]'s params selector", indexType.c_str());
        return nullptr;
    }

    util::KeyValueMap keyVal = schemaParameter.keyValMap;
    keyVal[DOC_NUM] = StringUtil::toString(docNum);
    if (!selector->InitSearcherParams(keyVal, indexParams)) {
        return nullptr;
    }

    string searcherName = selector->GetSearcherName();
    IndexSearcherPtr searcher = IndexFactory::CreateSearcher(searcherName);
    if (!searcher) {
        IE_LOG(ERROR, "failed to create searcher[%s]", searcherName.c_str());
        return nullptr;
    }

    int32_t ret = searcher->init(indexParams);
    if (ret < 0) {
        IE_LOG(ERROR, "failed to init, err[%s]", IndexError::What(ret));
        return nullptr;
    }

    signature = selector->GetOnlineSign(indexParams);
    enableGpu = selector->EnableGpuSearch(schemaParameter, docNum);

    return searcher;
}

}
}
