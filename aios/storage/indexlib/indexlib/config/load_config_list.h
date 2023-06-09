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
#ifndef __INDEXLIB_LOAD_CONFIG_LIST_H
#define __INDEXLIB_LOAD_CONFIG_LIST_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

typedef file_system::LoadConfigList LoadConfigList;
DEFINE_SHARED_PTR(LoadConfigList);

typedef file_system::LoadConfig LoadConfig;
DEFINE_SHARED_PTR(LoadConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_LOAD_CONFIG_LIST_H
