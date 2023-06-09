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
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_builder_factory.h"

using namespace std;
using namespace aitheta;
using namespace autil;
using namespace indexlib::util;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, KnnBuilderFactory);

IndexBuilderPtr KnnBuilderFactory::Create(const SchemaParameter &schemaParameter, const KeyValueMap &keyValMap,
                                          const KnnConfig &knnCfg, uint64_t docNum, MipsReformer &reformer) {
    IndexMeta meta;
    auto selector = KnnParamsSelectorFactory::CreateSelector(schemaParameter.indexType, schemaParameter, knnCfg, docNum);
    if (!selector) {
        IE_LOG(ERROR, "failed to get [%s]'s params selector ", schemaParameter.indexType.c_str());
        return IndexBuilderPtr();
    }

    if (!selector->InitMeta(keyValMap, meta)) {
        IE_LOG(ERROR, "failed to init knn meta");
        return IndexBuilderPtr();
    }

    IndexParams params;
    if (!selector->InitBuilderParams(keyValMap, params)) {
        IE_LOG(ERROR, "failed to init knn builder params");
        return IndexBuilderPtr();
    }

    reformer = MipsReformer(0u, 0.0f, 0.0f);
    if (selector->EnableMipsParams(keyValMap)) {
        MipsParams mipsParams;
        if (!selector->InitMipsParams(keyValMap, mipsParams)) {
            IE_LOG(ERROR, "failed to init MipsParams");
            return IndexBuilderPtr();
        }
        reformer = MipsReformer(mipsParams.mval, mipsParams.uval, mipsParams.norm);
    }

    string name = selector->GetBuilderName();
    auto builder = IndexFactory::CreateBuilder(name);
    if (nullptr == builder) {
        IE_LOG(ERROR, "create builder[%s] failed", name.data());
        return IndexBuilderPtr();
    }
    IE_LOG(INFO, "create builder[%s] success", name.data());

    int err = builder->init(meta, params);
    if (err < 0) {
        IE_LOG(ERROR, "init index[%s] failed, err[%s]", name.data(), IndexError::What(err));
        return IndexBuilderPtr();
    }
    return builder;
}

}
}
