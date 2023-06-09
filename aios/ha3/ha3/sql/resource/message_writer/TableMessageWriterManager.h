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

#include "ha3/sql/resource/MessageWriterManager.h"
#include "ha3/sql/resource/TableMessageWriter.h"
#include "autil/Lock.h"

namespace multi_call {
class SearchService;
} // namespace multi_call

namespace isearch {
namespace sql {

class TableMessageWriterManager : public MessageWriterManager {
public:
    TableMessageWriterManager(std::shared_ptr<multi_call::SearchService> searchService);
    ~TableMessageWriterManager();
    TableMessageWriterManager(const TableMessageWriterManager &) = delete;
    TableMessageWriterManager& operator=(const TableMessageWriterManager &) = delete;
public:
    bool init(const std::string &config) override;
    MessageWriter *getMessageWriter(const std::string &dbName,
                                    const std::string &tableName) override;
private:
    std::shared_ptr<multi_call::SearchService> _searchService;
    mutable autil::ReadWriteLock _lock;
    std::unordered_map<std::string, suez::RemoteTableWriterPtr> _remoteTableWriterMap;
    std::unordered_map<std::string, std::unordered_map<std::string, TableMessageWriterPtr>> _tableMessageWriterMap;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TableMessageWriterManager> TableMessageWriterManagerPtr;

} //end namespace sql
} //end namespace isearch
