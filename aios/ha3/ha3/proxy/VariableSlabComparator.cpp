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
#include "ha3/proxy/VariableSlabComparator.h"

#include <assert.h>

#include "autil/Log.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace suez::turing;

namespace isearch {
namespace proxy {
AUTIL_LOG_SETUP(ha3, VariableSlabComparator);

VariableSlabComparator::VariableSlabComparator() { 
}

VariableSlabComparator::~VariableSlabComparator() { 
}

VariableSlabComparatorPtr VariableSlabComparator::createComparator(const matchdoc::ReferenceBase *ref) {
    auto type = ref->getValueType().getBuiltinType();

#define CREATE_COMPARATOR(type)                                         \
    case type:                                                          \
    {                                                                   \
        typedef VariableTypeTraits<type, false>::AttrItemType T;        \
        const auto *typedRef = dynamic_cast<const matchdoc::Reference<T> *>(ref); \
        return VariableSlabComparatorPtr(new VariableSlabComparatorTyped<T>(typedRef)); \
    }                                                                   \

    switch(type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_COMPARATOR);
        CREATE_COMPARATOR(vt_string);
    default:
        assert(false);
        return VariableSlabComparatorPtr();
    }
#undef CREATE_COMPARATOR
    return VariableSlabComparatorPtr();
}

} // namespace proxy
} // namespace isearch

