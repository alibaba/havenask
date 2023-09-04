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
#include "sql/resource/TableMessageWriterManager.h"

#include <iosfwd>
#include <utility>
#include <vector>

#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "sql/common/Log.h"
#include "sql/config/TableWriteConfig.h"

namespace sql {
class MessageWriter;
} // namespace sql

using namespace std;
using namespace autil;

namespace sql {
AUTIL_LOG_SETUP(sql, TableMessageWriterManager);

TableMessageWriterManager::TableMessageWriterManager(multi_call::SearchServicePtr searchService)
    : _searchService(std::move(searchService)) {}

TableMessageWriterManager::~TableMessageWriterManager() {}

bool TableMessageWriterManager::init(const std::string &config) {
    TableWriteConfig tableWriteConfig;
    try {
        FastFromJsonString(tableWriteConfig, config);
    } catch (...) {
        SQL_LOG(WARN, "parse table write config failed [%s].", config.c_str());
        return false;
    }
    for (const auto &zoneName : tableWriteConfig.zoneNames) {
        suez::RemoteTableWriterPtr tableWriter(new suez::RemoteTableWriter(
            zoneName, _searchService.get(), tableWriteConfig.allowFollowWrite));
        _remoteTableWriterMap.emplace(zoneName, std::move(tableWriter));
    }
    SQL_LOG(INFO, "init table message writer manager success, config is [%s]", config.c_str());
    return true;
}

MessageWriter *TableMessageWriterManager::getMessageWriter(const std::string &dbName,
                                                           const std::string &tableName) {
    auto remoteIter = _remoteTableWriterMap.find(dbName);
    if (remoteIter == _remoteTableWriterMap.end()) {
        return nullptr;
    }
    {
        autil::ScopedReadLock lock(_lock);
        auto iter = _tableMessageWriterMap.find(dbName);
        if (iter != _tableMessageWriterMap.end()) {
            const auto &tableMsgWriterMap = iter->second;
            auto tableIter = tableMsgWriterMap.find(tableName);
            if (tableIter != tableMsgWriterMap.end()) {
                return tableIter->second.get();
            }
        }
    }
    {
        autil::ScopedWriteLock lock(_lock);
        auto &tableMsgWriterMap = _tableMessageWriterMap[dbName];
        auto tableIter = tableMsgWriterMap.find(tableName);
        if (tableIter != tableMsgWriterMap.end()) {
            return tableIter->second.get();
        } else {
            TableMessageWriterPtr writer(new TableMessageWriter(tableName, remoteIter->second));
            tableMsgWriterMap[tableName] = writer;
            return writer.get();
        }
    }
    return nullptr;
}

} // namespace sql
