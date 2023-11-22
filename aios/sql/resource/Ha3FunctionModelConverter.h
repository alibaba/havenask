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

#include "iquan/common/Status.h"

namespace iquan {
class FunctionModel;
class FunctionModels;
} // namespace iquan

namespace sql {
class Ha3FunctionModel;

class Ha3FunctionModelConverter {
public:
    static iquan::Status convert(std::vector<iquan::FunctionModel> &functionModels);
    static iquan::Status convert(iquan::FunctionModel &functionModel);
};

} // namespace sql
