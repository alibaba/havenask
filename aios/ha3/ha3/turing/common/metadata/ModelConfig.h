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
#pragma once

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"

namespace isearch {
namespace turing {

class NodeConfig : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("node", node);
        json.Jsonize("type", type);
        json.Jsonize("shape", shape, shape);
        json.Jsonize("default_params", defaultParams, defaultParams);
    }
public:
    std::string node;
    std::string type;
    std::vector<int> shape;
    std::vector<autil::legacy::Any> defaultParams;
};

class IndexNode : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("user", user);
        json.Jsonize("item", item);
        json.Jsonize("label", label);
    }
public:
    std::string user;
    std::string item;
    std::string label;
};

enum MODEL_TYPE {
    RECALL_MODEL = 0,
    SCORE_MODEL = 1,
    UNKNOWN_MODEL = 100,
};

class ModelInfo : public autil::legacy::Jsonizable {
public:
    ModelInfo()
    {
    }
public:
    bool empty() const {
        return inputs.empty() && outputs.empty() && index.empty();
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("input", inputs, inputs);
        json.Jsonize("output", outputs, outputs);
        json.Jsonize("index", index, index);
    }
public:
    std::vector<NodeConfig> inputs;
    std::vector<NodeConfig> outputs;
    std::vector<IndexNode> index;
};

class SamplingInfo : public autil::legacy::Jsonizable {
public:
    SamplingInfo()
        : count(100)
    {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("sampling_fields", fields);
        json.Jsonize("sampling_count", count);
        if (count <= 0) {
            count = 100;
        }
    }
    bool open() const {
        return !fields.empty();
    }
public:
    std::vector<std::string> fields;
    size_t count;
};

class ModelConfig : public autil::legacy::Jsonizable {
public:
    ModelConfig()
        : modelType(UNKNOWN_MODEL)
    {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == FROM_JSON) {
            std::string modelTypeStr;
            json.Jsonize("model_type", modelTypeStr);
            if (modelTypeStr == "recall") {
                modelType = MODEL_TYPE::RECALL_MODEL;
            } else if (modelTypeStr == "score") {
                modelType = MODEL_TYPE::SCORE_MODEL;
            }
        } else {
            throw autil::legacy::ExceptionBase("ModelConfig dose not support TO_JSON");
        }
        json.Jsonize("biz_name", bizName);
        json.Jsonize("zone_name", zoneName);
        json.Jsonize("qrs_model_info", qrsModelInfo, qrsModelInfo);
        json.Jsonize("searcher_model_info", searcherModelInfo);
        json.Jsonize("sampling_info", samplingInfo, samplingInfo);
    }
public:
    MODEL_TYPE modelType;
    std::string bizName;
    std::string zoneName;
    ModelInfo qrsModelInfo;
    ModelInfo searcherModelInfo;
    SamplingInfo samplingInfo;
};

typedef std::map<std::string, ModelConfig> ModelConfigMap;

} // namespace turing
} // namespace isearch
