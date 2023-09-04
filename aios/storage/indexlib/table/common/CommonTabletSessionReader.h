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

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"

namespace indexlibv2::table {

using TabletReader = framework::TabletReader;

template <typename ReaderImpl>
class CommonTabletSessionReader : public framework::ITabletReader
{
public:
    CommonTabletSessionReader(const std::shared_ptr<ReaderImpl>& readerImpl,
                              const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
        : _impl(readerImpl)
    {
        static_assert(std::is_base_of<TabletReader, ReaderImpl>::value, "expect an TabletReader");
        if (!memReclaimer) {
            return;
        }
        _memReclaimer = std::dynamic_pointer_cast<framework::EpochBasedMemReclaimer>(memReclaimer);
        if (_memReclaimer) {
            _criticalEpochItem = _memReclaimer->CriticalGuard();
        }
    }

    ~CommonTabletSessionReader()
    {
        if (_memReclaimer) {
            _memReclaimer->LeaveCritical(_criticalEpochItem);
        }
    }

public:
    std::shared_ptr<index::IIndexReader> GetIndexReader(const std::string& indexType,
                                                        const std::string& indexName) const override
    {
        return _impl->GetIndexReader(indexType, indexName);
    }

    std::shared_ptr<config::ITabletSchema> GetSchema() const override { return _impl->GetSchema(); }

    Status Search(const std::string& jsonQuery, std::string& result) const override
    {
        return _impl->Search(jsonQuery, result);
    }

public:
    template <typename IndexReaderType>
    std::shared_ptr<IndexReaderType> GetIndexReader(const std::string& indexType, const std::string& indexName) const
    {
        return _impl->template GetIndexReader<IndexReaderType>(indexType, indexName);
    }

protected:
    std::shared_ptr<ReaderImpl> _impl;

private:
    std::shared_ptr<framework::EpochBasedMemReclaimer> _memReclaimer;
    framework::EpochBasedMemReclaimer::EpochItem* _criticalEpochItem;
};

} // namespace indexlibv2::table
