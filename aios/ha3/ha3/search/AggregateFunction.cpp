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
#include "ha3/search/AggregateFunction.h"

#include <sstream>

#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, AggregateFunction);

AggregateFunction::AggregateFunction(const string &functionName,
                                     const std::string &parameter,
                                     VariableType resultType)
    : _functionName(functionName),
      _parameter(parameter)
{
    _resultType = resultType;
}

AggregateFunction::~AggregateFunction() {
}

} // namespace search
} // namespace isearch
