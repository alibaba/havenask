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
#include "sql/ops/scan/DocIdRangesReduceOptimize.h"

#include <rapidjson/document.h>
#include <cstddef>
#include <limits>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "matchdoc/ValueType.h"

#include "sql/common/common.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace indexlib::index_base;

namespace sql {

template<typename T>
KeyRangeTyped<T>* DocIdRangesReduceOptimize::genKeyRange(
        const std::string &attrName, const string &op,
        const SimpleValue &value)
{
    KeyRangeTyped<T> *keyRange(new KeyRangeTyped<T>(attrName));
    if (op == SQL_UDF_CONTAIN_OP) {
        const string &rawValueStr = value[1].GetString();
        string sepStr = "|";
        if (value.Size() == 3) {
            sepStr = value[2].GetString();
        }
        const vector<string> &rawValues =
            StringUtil::split(rawValueStr, sepStr, true);
        for (const auto &rawValue : rawValues) {
            T v = autil::StringUtil::fromString<T>(rawValue);
            keyRange->add(v, v);
        }
    } else if (op == SQL_IN_OP) {
        for (size_t i = 1; i < value.Size(); ++i) {
            T v = value[i].GetInt64();
            keyRange->add(v, v);
        }
    } else {
        T v = value.GetInt64();
        if (op == SQL_EQUAL_OP) {
            keyRange->add(v, v);
        } else if (op == SQL_GT_OP || op == SQL_GE_OP) {
            keyRange->add(v, std::numeric_limits<T>::max());
        } else if (op == SQL_LT_OP || op == SQL_LE_OP) {
            keyRange->add(std::numeric_limits<T>::min(), v);
        }
    }
    return keyRange;
}

}
