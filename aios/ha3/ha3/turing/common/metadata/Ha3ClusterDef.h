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

#include <string>
#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "ha3/turing/common/SortDescs.h"

namespace isearch {
namespace turing {

class Ha3ClusterHashModeDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        if (hashFields.empty() || hashFunction.empty()) {
            return false;
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("hash_field", hashField, hashField);
            json.Jsonize("hash_fields", hashFields, hashFields);
            json.Jsonize("hash_function", hashFunction, hashFunction);
            json.Jsonize("hash_params", hashParams, hashParams);

            if (hashFields.empty() && !hashField.empty()) {
                hashFields.push_back(hashField);
            }
        } else {
            throw iquan::IquanException("Ha3ClusterHashModeDef dose not support TO_JSON");
        }
    }

public:
    std::string hashField;
    std::vector<std::string> hashFields;
    std::string hashFunction;
    std::map<std::string, std::string> hashParams;
};

class Ha3BuildRuleConfigDef : public autil::legacy::Jsonizable {
public:
    Ha3BuildRuleConfigDef()
        : partitionCnt(1) {}

    bool isValid() const {
        return (partitionCnt >= 1);
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("partition_count", partitionCnt, partitionCnt);
        } else {
            throw iquan::IquanException("Ha3BuildRuleConfig dose not support TO_JSON");
        }
    }

public:
    int partitionCnt;
};

class Ha3ClusterConfigDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        return hashModeDef.isValid() && buildRuleConfigDef.isValid();
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("hash_mode", hashModeDef);
            json.Jsonize("builder_rule_config", buildRuleConfigDef,
                         buildRuleConfigDef);
        } else {
            throw iquan::IquanException("Ha3ClusterConfigDef dose not support TO_JSON");
        }
    }

public:
    Ha3ClusterHashModeDef hashModeDef;
    Ha3BuildRuleConfigDef buildRuleConfigDef;
};

class Ha3BuildOptionDef : public autil::legacy::Jsonizable {
public:
    Ha3BuildOptionDef()
        : sortBuild(false)
    {
    }
    bool isValid() const {
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("sort_build", sortBuild, sortBuild);
            if (sortBuild) {
                json.Jsonize("sort_descriptions", sortDescs, sortDescs);
            }
        } else {
            throw iquan::IquanException("Ha3BuildOptionDef dose not support TO_JSON");
        }
    }

public:
    bool sortBuild;
    std::vector<Ha3SortDesc> sortDescs;
};

class Ha3ClusterDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        return clusterConfigDef.isValid();
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("cluster_config", clusterConfigDef);
            json.Jsonize("build_option_config", buildOptionDef, buildOptionDef);
        } else {
            throw iquan::IquanException("Ha3ClusterDef dose not support TO_JSON");
        }
    }

public:
    Ha3ClusterConfigDef clusterConfigDef;
    Ha3BuildOptionDef buildOptionDef;
};

} // namespace turing
} // namespace isearch
