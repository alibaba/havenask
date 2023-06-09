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
#include "navi/ops/ResourceKernelDef.h"

namespace navi {

const std::string RESOURCE_CREATE_KERNEL = "navi@r_create";
const std::string RESOURCE_CREATE_OUTPUT = "out";
const std::string RESOURCE_ATTR_NAME = "name";
const std::string RESOURCE_ATTR_REQUIRE = "require";

const std::string RESOURCE_SAVE_KERNEL = "navi@r_save";
const std::string RESOURCE_SAVE_KERNEL_INPUT = "in";
const std::string RESOURCE_SAVE_KERNEL_OUTPUT = "out";

}
