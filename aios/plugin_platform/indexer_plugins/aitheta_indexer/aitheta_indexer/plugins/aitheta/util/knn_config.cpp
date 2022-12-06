/**
 * @file   knn_dynamic_config.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Fri Jan 18 10:37:34 2019
 *
 * @brief
 *
 *
 */

#include <algorithm>
#include <indexlib/storage/file_system_wrapper.h>
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"
#include "aitheta_indexer/plugins/aitheta/util/param_util.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
FSLIB_USE_NAMESPACE(fs);

IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, KnnRangeParams);

KnnRangeParams::KnnRangeParams() : upperLimit(0) {}

KnnRangeParams::~KnnRangeParams() {}

void KnnRangeParams::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("upper_limit", upperLimit, upperLimit);
    json.Jsonize("params", params, params);
}

void KnnRangeParams::Merge(const KnnRangeParams &otherParams) { ParamUtil::MergeParams(otherParams.params, params); }

IE_LOG_SETUP(aitheta_plugin, KnnStrategy);
KnnStrategy::KnnStrategy() : useAsDefault(false) {}

KnnStrategy::~KnnStrategy() {}

void KnnStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("name", name, name);
    json.Jsonize("as_default", useAsDefault, false);
    json.Jsonize("dynamic_param_list", rangeParamsList, rangeParamsList);
    if (FROM_JSON == json.GetMode()) {
        sort(rangeParamsList.begin(), rangeParamsList.end());
    }
}

bool KnnStrategy::IsAvailable() {
    for (size_t i = 1; i < rangeParamsList.size(); ++i) {
        if (rangeParamsList[i].upperLimit <= rangeParamsList[i - 1].upperLimit) {
            return false;
        }
    }

    for (size_t i = 0; i < rangeParamsList.size(); ++i) {
        if (!rangeParamsList[i].IsAvailable()) {
            return false;
        }
    }
    return true;
}

IE_LOG_SETUP(aitheta_plugin, KnnStrategies);

KnnStrategies::KnnStrategies() {}

KnnStrategies::~KnnStrategies() {}

void KnnStrategies::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("type", type, type);
    if (FROM_JSON == json.GetMode()) {
        vector<KnnStrategy> strategies;
        json.Jsonize("strategies", strategies, strategies);
        map<string, KnnStrategy>::iterator itr;
        for (auto strategy : strategies) {
            string name = strategy.name;
            itr = strategy2RangeParams.find(name);
            if (itr == strategy2RangeParams.end()) {
                strategy2RangeParams.insert(pair<string, KnnStrategy>(name, strategy));
            }

            if (strategy.useAsDefault) {
                itr = strategy2RangeParams.find(DEFAULT_STRATEGY);
                if (itr == strategy2RangeParams.end()) {
                    strategy.setName(DEFAULT_STRATEGY);
                    strategy2RangeParams.insert(pair<string, KnnStrategy>(DEFAULT_STRATEGY, strategy));
                }
            }
        }
    } else {
        vector<Any> anys;
        for (auto strategy : strategy2RangeParams) {
            anys.push_back(ToJson(strategy.second));
        }
        json.Jsonize("strategies", anys);
    }
}

bool KnnStrategies::IsAvailable() {
    for (auto config : strategy2RangeParams) {
        if (!config.second.IsAvailable()) {
            return false;
        }
    }

    if (strategy2RangeParams.end() == strategy2RangeParams.find(DEFAULT_STRATEGY)) {
        // 没有默认的default param,报错
        return false;
    }
    return true;
}

IE_LOG_SETUP(aitheta_plugin, KnnConfig);

KnnConfig::KnnConfig() {}

KnnConfig::~KnnConfig() {}

void KnnConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (FROM_JSON == json.GetMode()) {
        vector<KnnStrategies> typeStrategies;
        json.Jsonize("dynamic_configs", typeStrategies, typeStrategies);
        map<string, KnnStrategies>::iterator itr;
        for (auto strategies : typeStrategies) {
            string type = strategies.type;
            itr = type2Strategies.find(type);
            if (itr != type2Strategies.end()) {
                continue;
            }
            type2Strategies.insert(pair<string, KnnStrategies>(type, strategies));
        }
    } else {
        vector<Any> anys;
        for (auto strategies : type2Strategies) {
            anys.push_back(ToJson(strategies.second));
        }
        json.Jsonize("dynamic_configs", anys);
    }
}

bool KnnConfig::IsAvailable() const {
    for (auto strategies : type2Strategies) {
        if (!strategies.second.IsAvailable()) {
            IE_LOG(WARN, "type[%s]'s strategies is invailable", strategies.first.c_str());
            return false;
        }
    }
    return true;
}

bool KnnConfig::Load(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) {
    auto reader = IndexlibIoWrapper::CreateReader(directory, KNN_GLOBAL_CONFIG, FSOT_IN_MEM);
    if (!reader) {
        IE_LOG(ERROR, "file[%s] does not exist in path[%s]", KNN_GLOBAL_CONFIG.c_str(), directory->GetPath().c_str());
        return false;
    }

    if (!reader->GetLength()) {
        IE_LOG(ERROR, "file[%s] is empty in path[%s]", KNN_GLOBAL_CONFIG.c_str(), directory->GetPath().c_str());
        return false;
    }

    string content((const char *)reader->GetBaseAddress(), reader->GetLength());
    try {
        FromJsonString(*this, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        IE_LOG(ERROR, "failed to deserialize. Error:[%s], content:[%s].", e.what(), content.c_str());
        return false;
    }

    return IsAvailable();
}

bool KnnConfig::Load(const std::string &path) {
    auto dir = IndexlibIoWrapper::CreateDirectory(path);
    if (nullptr == dir) {
        return false;
    }
    return Load(dir);
}

bool KnnConfig::Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) const {
    directory->RemoveFile(KNN_GLOBAL_CONFIG, true);
    if (IsAvailable()) {
        string content = ToJsonString(*this);

        auto writer = IndexlibIoWrapper::CreateWriter(directory, KNN_GLOBAL_CONFIG);
        if (!writer) {
            IE_LOG(ERROR, "failed to create file[%s] in path[%s]", KNN_GLOBAL_CONFIG.c_str(),
                   directory->GetPath().c_str());
            return false;
        }
        writer->Write(content.c_str(), content.size());
        writer->Close();
    }
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
