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

#include "indexlib/index/common/PrimaryKeyHashType.h"

enum PrimaryKeyIndexType {
    pk_sort_array,
    pk_hash_table,
    pk_block_array,
};

namespace indexlibv2::config {
inline const std::string USE_NUMBER_PK_HASH = "use_number_pk_hash";
inline const std::string PRIMARY_KEY_STORAGE_TYPE = "pk_storage_type";
} // namespace indexlibv2::config
