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

#include <string>

#include "indexlib/index/ann/Common.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"

namespace indexlib::table {
inline const std::string NORMAL_TABLET_INFO_HOLDER = "normal_tablet_info_holder";
inline const std::string NORMAL_TABLE_GROUP_CONFIG_KEY = "segment_group_config";
inline const std::string NORMAL_TABLE_GROUP_TAG_KEY = "segment_group";
inline const std::string NORMAL_TABLE_COMPACTION_TYPE = "compaction_type";
inline const std::string NORMAL_TABLE_SPLIT_TYPE = "split";
inline const std::string NORMAL_TABLE_MERGE_TYPE = "merge";
} // namespace indexlib::table
//////////////////////////////////////////////////////////////////////
namespace indexlibv2::table {
using indexlib::table::NORMAL_TABLE_COMPACTION_TYPE;
using indexlib::table::NORMAL_TABLE_GROUP_CONFIG_KEY;
using indexlib::table::NORMAL_TABLE_GROUP_TAG_KEY;
using indexlib::table::NORMAL_TABLE_MERGE_TYPE;
using indexlib::table::NORMAL_TABLE_SPLIT_TYPE;
using indexlib::table::NORMAL_TABLET_INFO_HOLDER;
} // namespace indexlibv2::table
