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
#include "indexlib/file_system/load_config/LoadStrategy.h"

namespace indexlib { namespace file_system {

const std::string READ_MODE_MMAP = "mmap";
const std::string READ_MODE_CACHE = "cache";
const std::string READ_MODE_GLOBAL_CACHE = "global_cache";
const std::string READ_MODE_MEM = "mem";

AUTIL_LOG_SETUP(indexlib.file_system, LoadStrategy);

LoadStrategy::LoadStrategy() {}
LoadStrategy::~LoadStrategy() {}

}} // namespace indexlib::file_system
