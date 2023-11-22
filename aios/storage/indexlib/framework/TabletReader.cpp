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
#include "indexlib/framework/TabletReader.h"

#include <stddef.h>
#include <typeinfo>

#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletReader);

TabletReader::TabletReader(const std::shared_ptr<config::ITabletSchema>& schema) : _schema(schema) {}
TabletReader::~TabletReader()
{
    for (auto iter = _indexReaderMap.begin(); iter != _indexReaderMap.end(); iter++) {
        size_t useCount = iter->second.use_count();
        if (useCount > 1) {
            AUTIL_LOG(WARN, "unreleased index reader, indexName[%s] indexType[%s] use count [%ld]",
                      iter->first.second.c_str(), iter->first.first.c_str(), useCount);
        }
    }
}

Status TabletReader::Open(const std::shared_ptr<TabletData>& tabletData, const ReadResource& readResource)
{
    _indexMemoryReclaimer = readResource.indexMemoryReclaimer;
    return DoOpen(tabletData, readResource);
}

Status TabletReader::Search(const std::string& jsonQuery, std::string& result) const
{
    return Status::Unimplement("search query [%s] fail.\n"
                               "not support Search interface in class [%s]",
                               jsonQuery.c_str(), typeid(*this).name());
}

std::shared_ptr<config::ITabletSchema> TabletReader::GetSchema() const { return _schema; }

std::shared_ptr<index::IIndexReader> TabletReader::GetIndexReader(const std::string& indexType,
                                                                  const std::string& indexName) const
{
    auto key = make_pair(indexType, indexName);
    auto it = _indexReaderMap.find(key);
    if (it == _indexReaderMap.end()) {
        return nullptr;
    } else {
        return it->second;
    }
}

} // namespace indexlibv2::framework
