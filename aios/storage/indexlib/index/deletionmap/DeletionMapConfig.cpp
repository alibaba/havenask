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
#include "indexlib/index/deletionmap/DeletionMapConfig.h"

#include "indexlib/index/deletionmap/Common.h"

namespace indexlibv2::index {

const std::string& DeletionMapConfig::GetIndexName() const { return DELETION_MAP_INDEX_NAME; }

const std::string& DeletionMapConfig::GetIndexType() const { return DELETION_MAP_INDEX_TYPE_STR; }
const std::string& DeletionMapConfig::GetIndexCommonPath() const { return DELETION_MAP_INDEX_PATH; }
std::vector<std::string> DeletionMapConfig::GetIndexPath() const { return {GetIndexCommonPath()}; }

} // namespace indexlibv2::index
