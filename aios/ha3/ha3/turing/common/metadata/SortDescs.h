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
#include "iquan/common/IquanException.h"

namespace isearch {
namespace turing {

class Ha3SortDesc : public autil::legacy::Jsonizable {
public:
    Ha3SortDesc(const std::string &field_ = "", const std::string &order_ = "")
        : field(field_)
        , order(order_)
    {
    }
public:
    bool isValid() const {
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("sort_field", field);
            json.Jsonize("sort_pattern", order, order);
        } else {
            throw iquan::IquanException("Ha3SortDesc dose not support TO_JSON");
        }
    }

public:
    std::string field;
    std::string order;
};

} // namespace turing
} // namespace isearch
