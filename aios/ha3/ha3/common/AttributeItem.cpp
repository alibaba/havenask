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
#include "ha3/common/AttributeItem.h"

#include <tr1/type_traits>

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
using namespace std::tr1;
using namespace suez::turing;

using namespace isearch::util;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AttributeItem);

AttributeItem* AttributeItem::createAttributeItem(VariableType variableType,
        bool isMulti)
{
    AttributeItem *ret = NULL;
#define CREATE_ATTRIBUTE_ITEM_CASE_HELPER(vt_type)                      \
    case vt_type:                                                       \
        if (isMulti) {                                                  \
            ret = new AttributeItemTyped<VariableTypeTraits<vt_type, true>::AttrItemType>; \
        } else {                                                        \
            ret = new AttributeItemTyped<VariableTypeTraits<vt_type, false>::AttrItemType>; \
        }                                                               \
        break

    switch(variableType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_ATTRIBUTE_ITEM_CASE_HELPER);
        CREATE_ATTRIBUTE_ITEM_CASE_HELPER(vt_string);
    default:
        break;
    }
    return ret;
}

} // namespace common
} // namespace isearch
