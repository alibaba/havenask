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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::framework {

class TabletReader : public ITabletReader
{
public:
    explicit TabletReader(const std::shared_ptr<config::ITabletSchema>& schema);
    ~TabletReader();

public:
    Status Open(const std::shared_ptr<TabletData>& tabletData, const ReadResource& readResource);

public:
    Status Search(const std::string& jsonQuery, std::string& result) const override;
    std::shared_ptr<config::ITabletSchema> GetSchema() const override;
    std::shared_ptr<index::IIndexReader> GetIndexReader(const std::string& indexType,
                                                        const std::string& indexName) const override;
    template <typename IndexReaderType>
    std::shared_ptr<IndexReaderType> GetIndexReader(const std::string& indexType, const std::string& indexName) const;

protected:
    virtual Status DoOpen(const std::shared_ptr<TabletData>& tabletData, const ReadResource& readResource) = 0;

protected:
    using IndexReaderMapKey = std::pair<std::string, std::string>;

    std::shared_ptr<config::ITabletSchema> _schema;
    std::map<IndexReaderMapKey, std::shared_ptr<index::IIndexReader>> _indexReaderMap;
    std::shared_ptr<IIndexMemoryReclaimer> _indexMemoryReclaimer;

private:
    AUTIL_LOG_DECLARE();
};

template <typename IndexReaderType>
inline std::shared_ptr<IndexReaderType> TabletReader::GetIndexReader(const std::string& indexType,
                                                                     const std::string& indexName) const
{
    return std::dynamic_pointer_cast<IndexReaderType>(GetIndexReader(indexType, indexName));
}

} // namespace indexlibv2::framework
