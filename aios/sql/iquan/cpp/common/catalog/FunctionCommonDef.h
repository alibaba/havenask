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

#include <map>
#include <string>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

namespace iquan {

class ParamTypeDef : public autil::legacy::Jsonizable {
public:
    ParamTypeDef()
        : isMulti(false) {}

    ParamTypeDef(bool isMulti, const std::string &type)
        : isMulti(isMulti)
        , type(type) {
        if (isMulti) {
            this->valueType["type"] = autil::legacy::Any(type);
            this->type = "array";
        }
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == TO_JSON) {
            json.Jsonize("type", type);
            if (extendInfos.size() > 0) {
                json.Jsonize("extend_infos", extendInfos);
            }

            if (keyType.size() > 0) {
                json.Jsonize("key_type", keyType);
            }

            if (valueType.size() > 0) {
                json.Jsonize("value_type", valueType);
            }
        } else {
            json.Jsonize("is_multi", isMulti, false);
            json.Jsonize("type", type, std::string());
            json.Jsonize("extend_infos", extendInfos, {});
            json.Jsonize("key_type", keyType, {});
            json.Jsonize("value_type", valueType, {});

            if (type == "ARRAY" || type == "array") {
                isMulti = true;
            }
        }
    }

    bool isValid() const {
        if (type.empty()) {
            return false;
        }
        return true;
    }

public:
    bool isMulti;
    std::string type;
    std::map<std::string, std::string> extendInfos;
    autil::legacy::json::JsonMap keyType;
    autil::legacy::json::JsonMap valueType;
};

} // namespace iquan
