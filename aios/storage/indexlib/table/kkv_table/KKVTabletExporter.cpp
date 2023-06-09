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
#include "indexlib/table/kkv_table/KKVTabletExporter.h"

#include "indexlib/table/kkv_table/KKVTabletDocIterator.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTabletExporter);

KKVTabletExporter::KKVTabletExporter() {}

KKVTabletExporter::~KKVTabletExporter() {}

std::optional<std::string> KKVTabletExporter::CreateTabletOptionsString()
{
    std::string onlineConfigStr = R"({
    "online_index_config":{
        "need_read_remote_index":true,
        "load_config":[
            {
                "file_patterns":[
                    "_KKV_PKEY_"
                ],
                "name":"pkey_strategy",
                "load_strategy":"mmap",
                "load_strategy_param":{
                    "slice":4194304,
                    "lock":true,
                    "interval":0
                },
                "remote":true,
                "deploy":false
            },
            {
                "file_patterns":[
                    ".*"
                ],
                "name":"default_strategy",
                "load_strategy":"cache",
                "load_strategy_param":{
                    "cache_decompress_file":true,
                    "direct_io":false
                },
                "remote":true,
                "deploy":false
            }
        ]
    }
    })";
    return onlineConfigStr;
}

std::unique_ptr<indexlibv2::framework::ITabletDocIterator> KKVTabletExporter::CreateTabletDocIterator()
{
    return std::make_unique<indexlibv2::table::KKVTabletDocIterator>();
}

} // namespace indexlib::table
