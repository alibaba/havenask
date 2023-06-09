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

#include "autil/NoCopyable.h"
#include "future_lite/Future.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/OpenOptions.h"
#include "indexlib/framework/SegmentDumper.h"
#include "indexlib/framework/Version.h"

namespace autil {
class ThreadPool;
}
namespace indexlibv2::document {
class IDocumentBatch;
}
namespace indexlibv2::framework {

class TabletData;

// base class of all customized tablet
class TabletWriter : private autil::NoCopyable
{
public:
    virtual ~TabletWriter() = default;

    virtual Status Open(const std::shared_ptr<TabletData>& data, const BuildResource& buildResource,
                        const OpenOptions& openOptions) = 0;
    // Return Status:
    // 1. OK: build success
    // 2. NoMem: build no mem, wait mem free
    // 3. NeedDump: build trigger dump, need dump and reopen
    virtual Status Build(const std::shared_ptr<document::IDocumentBatch>& batch) = 0;
    virtual std::unique_ptr<SegmentDumper> CreateSegmentDumper() = 0;
    virtual size_t GetTotalMemSize() const = 0;
    virtual size_t GetBuildingSegmentDumpExpandSize() const = 0;
    virtual void ReportMetrics() = 0;
    virtual bool IsDirty() const = 0;
};

} // namespace indexlibv2::framework
