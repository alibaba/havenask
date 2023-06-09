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

#include "autil/Log.h"
#include "indexlib/index/IIndexReader.h"

namespace indexlib::index {
class OperationLogIndexer;
class OperationLogReplayer;
class OperationLogConfig;

class OperationLogIndexReader : public indexlibv2::index::IIndexReader
{
public:
    OperationLogIndexReader();
    ~OperationLogIndexReader();

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override;
    std::unique_ptr<OperationLogReplayer> CreateReplayer();

private:
    std::vector<std::shared_ptr<OperationLogIndexer>> _indexers;
    std::shared_ptr<OperationLogConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
