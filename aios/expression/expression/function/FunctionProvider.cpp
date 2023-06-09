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
#include "expression/function/FunctionProvider.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, FunctionProvider);

FunctionProvider::FunctionProvider(const FunctionResource &resource)
    : SessionResourceProvider(resource)
    , _attrExprCreator(resource.attrExprCreator)
{
}

FunctionProvider::~FunctionProvider() {
}

matchdoc::ReferenceBase* FunctionProvider::requireAttribute(const string &name, SerializeLevel sl) {
    if (getLocation() == FL_UNKNOWN) {
        return NULL;
    }
    if (getLocation() == FL_PROXY) {
        return _sessionResource->allocator->findReferenceWithoutType(name);
    }
    // searcher
    AttributeExpression *expr = _attrExprCreator->create(name);
    if (!expr) {
        return NULL;
    }
    if (!expr->allocate(_sessionResource->allocator.get())) {
        return NULL;
    }
    _attrExprCreator->addNeedEvalAttributeExpression(expr);
    matchdoc::ReferenceBase *ref = expr->getReferenceBase();
    ref->setSerializeLevel(sl);
    return ref;
}
}
