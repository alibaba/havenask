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
#include "suez/sdk/SourceReaderProvider.h"

#include "alog/Logger.h"
#include "autil/Log.h"
#include "suez/sdk/PartitionId.h"

using namespace std;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SourceReaderProvider);

SourceReaderProvider::SourceReaderProvider() {}

SourceReaderProvider::~SourceReaderProvider() {}

void SourceReaderProvider::erase(const PartitionId &pid) {
    string tableName = pid.getTableName();
    auto iter = sourceConfigs.find(tableName);
    if (iter != sourceConfigs.end()) {
        auto &pid2Config = iter->second;
        pid2Config.erase(pid);
        if (pid2Config.empty()) {
            sourceConfigs.erase(tableName);
        }
    }
}

void SourceReaderProvider::clear() { sourceConfigs.clear(); }

bool SourceReaderProvider::addSourceConfig(PartitionSourceReaderConfig config) {
    auto pid = config.pid;
    auto ret = sourceConfigs[pid.getTableName()].emplace(pid, std::move(config));
    if (!ret.second) {
        AUTIL_LOG(ERROR, "source reader conflict for pid[%s]", autil::legacy::FastToJsonString(pid).c_str());
        return false;
    }
    return true;
}

const PartitionSourceReaderConfig *SourceReaderProvider::getSourceConfigByIdx(const std::string &tableName,
                                                                              int32_t index) const {
    auto iter = sourceConfigs.find(tableName);
    if (iter == sourceConfigs.end()) {
        AUTIL_LOG(ERROR, "table [%s] not found", tableName.c_str());
    } else {
        for (const auto &pair : iter->second) {
            if (pair.first.partCount == 1) { // broadcast table has only one partition
                return &pair.second;
            }
            if (pair.first.index == index) {
                return &pair.second;
            }
        }
    }
    AUTIL_LOG(ERROR, "table [%s] found, index [%d] not found", tableName.c_str(), index);
    return nullptr;
}

} // namespace suez
