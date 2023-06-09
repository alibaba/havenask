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

#include <vector>

#include "autil/Log.h"
#include "autil/result/Errors.h"
#include "indexlib/base/Status.h"
#include "indexlib/table/common/CommonTabletWriter.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/kkv_table/KKVTabletReader.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::table {

class KKVTabletWriter : public table::CommonTabletWriter
{
public:
    KKVTabletWriter(const std::shared_ptr<config::TabletSchema>& schema, const config::TabletOptions* options);
    ~KKVTabletWriter();

protected:
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions) override;

private:
    virtual std::shared_ptr<KKVTabletReader> CreateKKVTabletReader();
    Status OpenTabletReader();
    Status RewriteDocument(document::IDocumentBatch* batch);

    std::shared_ptr<KKVReader> GetKKVReader(uint64_t indexNameHash) const
    {
        if (_kkvReaders.size() == 1 && indexNameHash == 0) {
            return _kkvReaders.begin()->second;
        }
        if (auto it = _kkvReaders.find(indexNameHash); it != _kkvReaders.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    indexlib::util::SimplePool _simplePool;
    std::shared_ptr<KKVTabletReader> _tabletReader;
    std::map<uint64_t, std::shared_ptr<KKVReader>> _kkvReaders;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
