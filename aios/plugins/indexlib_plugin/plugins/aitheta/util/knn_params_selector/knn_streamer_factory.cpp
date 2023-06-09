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
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_streamer_factory.h"

using namespace std;
using namespace aitheta;
using namespace autil;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, KnnStreamerFactory);

IndexStreamerPtr KnnStreamerFactory::Create(const SchemaParameter &schemaParam, const KnnConfig &knnCfg,
                                            const IndexAttr &indexAttr, IndexParams &params, int64_t &signature) {
    string indexType = schemaParam.indexType;
    uint32_t docNum = indexAttr.docNum;

    IKnnParamsSelectorPtr selector =
        KnnParamsSelectorFactory::CreateSelector(indexType, schemaParam, knnCfg, docNum, true);
    if (!selector) {
        IE_LOG(ERROR, "failed to get [%s]'s params selector", indexType.c_str());
        return nullptr;
    }

    util::KeyValueMap keyValMap = schemaParam.keyValMap;

    auto &reformer = indexAttr.reformer;
    if (reformer.m() <= 0u || reformer.norm() <= 0.0f) {
        keyValMap[MIPS_ENABLE] = "false";
        IE_LOG(INFO, "mips transfer is disabled with streamer");
    } else {
        keyValMap[MIPS_ENABLE] = "true";
        keyValMap[MIPS_PARAM_MVAL] = StringUtil::toString(reformer.m());
        IE_LOG(INFO, "udpate mips dimension val to [%u]", reformer.m());
    }

    IndexMeta meta;
    if (!selector->InitMeta(keyValMap, meta)) {
        IE_LOG(ERROR, "failed to init knn meta");
        return nullptr;
    }
    if (!selector->InitStreamerParams(keyValMap, params)) {
        IE_LOG(ERROR, "failed to init knn builder params");
        return nullptr;
    }

    string name = selector->GetStreamerName();
    auto streamer = IndexFactory::CreateStreamer(name);
    if (!streamer) {
        IE_LOG(ERROR, "failed to create streamer[%s] ", name.c_str());
        return nullptr;
    }

    int32_t err = streamer->init(meta, params);
    if (err != 0) {
        IE_LOG(ERROR, "failed to init index[%s], err[%s]", name.c_str(), IndexError::What(err));
        return nullptr;
    }

    static const string kDummyPath;
    err = streamer->open(kDummyPath);
    if (err != 0) {
        IE_LOG(ERROR, "failed to open dummy path in streamer with [%s]", IndexError::What(err));
        return nullptr;
    }

    signature = selector->GetOnlineSign(params);

    return streamer;
}

}
}
