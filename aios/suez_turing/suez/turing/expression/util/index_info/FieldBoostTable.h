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

#include <stdint.h>
#include <vector>

#include "autil/Log.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/FieldBoost.h"

namespace suez {
namespace turing {

typedef uint32_t fieldbitmap_t;
const fieldbitmap_t INVALID_FIELDBITMAP = (fieldbitmap_t)0xFFFFFFFF;

class FieldBoostTable {
public:
    FieldBoostTable();
    ~FieldBoostTable();

    fieldboost_t getFieldBoost(indexid_t indexid, int32_t fieldPosition) const;
    fieldboost_t getFieldBoostSum(indexid_t indexid, fieldbitmap_t bitmap) const;
    fieldboost_t getFieldBoostMax(indexid_t indexid, fieldbitmap_t bitmap) const;

    void setFieldBoost(indexid_t indexid, const FieldBoost &fieldbitboost);
    void setFieldBoostValue(indexid_t indexid, int32_t fieldPosition, fieldboost_t boostValue);
    void clear();

private:
    std::vector<FieldBoost> _boostTable;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
