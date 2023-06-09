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
#include "autil/NoCopyable.h"
#include "indexlib/document/IDocument.h"

namespace indexlibv2::document {
class RawDocument;
}

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class TabletData;
class MetricsManager;

class ITabletDocIterator : private autil::NoCopyable
{
public:
    virtual ~ITabletDocIterator() = default;

public:
    using CreateDocIteratorFunc = std::function<std::shared_ptr<framework::ITabletDocIterator>(
        const std::shared_ptr<indexlibv2::config::TabletSchema>&)>;

public:
    virtual Status Init(const std::shared_ptr<TabletData>& tabletData,
                        std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                        const std::shared_ptr<MetricsManager>& metricsManager,
                        const std::map<std::string, std::string>& params) = 0;
    virtual Status Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                        document::IDocument::DocInfo* docInfo) = 0;
    virtual bool HasNext() const = 0;
    virtual Status Seek(const std::string& checkpoint) = 0;
};

} // namespace indexlibv2::framework
