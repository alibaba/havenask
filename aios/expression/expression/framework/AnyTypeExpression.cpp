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
#include "expression/framework/AnyTypeExpression.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

namespace expression {
AUTIL_LOG_SETUP(expression, AnyTypeExpression);

AnyTypeExpression::AnyTypeExpression(const string &exprName)
    : AttributeExpression(ET_ANY, EVT_UNKNOWN, false, exprName)
    , _data(NULL)
    , _len(0)
{
}

AnyTypeExpression::~AnyTypeExpression() {
    _data = NULL;
    _len = 0;
}

bool AnyTypeExpression::allocate(MatchDocAllocator *allocator,
                                 const string &groupName,
                                 uint8_t serializeLevel)
{
    // not support, can not determine type
    return true;
}

void AnyTypeExpression::evaluate(const MatchDoc &matchDoc) {
    assert(false);
}

void AnyTypeExpression::batchEvaluate(MatchDoc *matchDocs, uint32_t docCount) {
    assert(false);
}

void AnyTypeExpression::reset() {
    _data = NULL;
    _len = 0;
    _exprValueType = EVT_UNKNOWN;
    _isMulti = false;
}

matchdoc::ReferenceBase* AnyTypeExpression::getReferenceBase() const {
    return NULL;
}
}
