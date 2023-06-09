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
#include "indexlib/table/normal_table/NormalTabletExporter.h"

#include <limits.h>
#include <unistd.h>

#include "autil/StringUtil.h"
#include "indexlib/table/normal_table/NormalTabletDocIterator.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletExporter);

NormalTabletExporter::NormalTabletExporter() {}

NormalTabletExporter::~NormalTabletExporter() {}

std::optional<std::string> NormalTabletExporter::CreateTabletOptionsString()
{
    std::string onlineConfigStr = R"({
    "online_index_config":{
        "need_read_remote_index":true,
        "build_config":{
            "speedup_primary_key_reader":true
        },
        "load_config":[
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
    },
    "export_loader":true,
    "export_loader_workpath":"$CWD"
    })";

    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        AUTIL_LOG(ERROR, "get current path failed");
        return std::nullopt;
    }

    autil::StringUtil::replaceAll(onlineConfigStr, "$CWD", std::string(cwdPath));
    return onlineConfigStr;
}

std::unique_ptr<indexlibv2::framework::ITabletDocIterator> NormalTabletExporter::CreateTabletDocIterator()
{
    return std::make_unique<indexlibv2::table::NormalTabletDocIterator>();
}

} // namespace indexlib::table
