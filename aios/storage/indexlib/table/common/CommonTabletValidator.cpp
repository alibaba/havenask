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
#include "indexlib/table/common/CommonTabletValidator.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletCreator.h"
#include "indexlib/framework/TabletId.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, CommonTabletValidator);

Status CommonTabletValidator::Validate(const std::string& indexRootPath,
                                       const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                                       versionid_t versionId)
{
    auto memoryQuotaController = std::make_shared<indexlibv2::MemoryQuotaController>(
        /*name*/ "validator", /*totalQuota*/ std::numeric_limits<int64_t>::max());
    auto fileBlockCacheContainer = std::make_shared<file_system::FileBlockCacheContainer>();
    if (!fileBlockCacheContainer->Init(/*configStr*/ "", memoryQuotaController,
                                       /*taskscheduler*/ nullptr, /*metricProvider*/ nullptr)) {
        AUTIL_LOG(ERROR, "init file block cache failed");
        return Status::InternalError("init file block cache failed");
    }

    auto tablet = indexlibv2::framework::TabletCreator()
                      .SetTabletId(framework::TabletId("validator"))
                      .SetMemoryQuotaController(memoryQuotaController, /*buildMemoryQuotaController*/ nullptr)
                      .SetFileBlockCacheContainer(fileBlockCacheContainer)
                      .SetRegisterTablet(false)
                      .CreateTablet();
    if (!tablet) {
        AUTIL_LOG(ERROR, "create validator tablet failed");
        return Status::InternalError("create validator tablet failed");
    }

    std::string onlineConfigStr = R"({
    "load_index_for_check" : true,
    "online_index_config":{
        "need_read_remote_index":true,
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
    }
    })";

    auto options = std::make_shared<indexlibv2::config::TabletOptions>();
    FromJsonString(*options, onlineConfigStr);
    options->SetIsLeader(false);
    options->SetFlushRemote(false);
    options->SetFlushLocal(false);
    options->SetIsOnline(true);
    options->MutableBackgroundTaskConfig().DisableAll();

    indexlibv2::framework::IndexRoot indexRoot(indexRootPath, indexRootPath);
    auto status = tablet->Open(indexRoot, schema, options, versionId);
    RETURN_IF_STATUS_ERROR(status, "open tablet[%s] on version[%d] failed", indexRootPath.c_str(), versionId);
    tablet->Close();

    return Status::OK();
}

} // namespace indexlib::table
