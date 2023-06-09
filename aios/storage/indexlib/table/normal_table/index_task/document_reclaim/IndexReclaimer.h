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
#include "indexlib/base/Status.h"
#include "indexlib/framework/MetricsWrapper.h"

namespace indexlibv2::index {
class DeletionMapPatchWriter;
} // namespace indexlibv2::index

namespace indexlib::index {
class MultiFieldIndexReader;
} // namespace indexlib::index

namespace indexlibv2::table {

class IndexReclaimer : private autil::NoCopyable
{
public:
    IndexReclaimer(const std::shared_ptr<kmonitor::MetricsReporter>& reporter) : _metricsReporter(reporter) {}
    virtual ~IndexReclaimer() = default;

public:
    virtual Status Reclaim(index::DeletionMapPatchWriter* docDeleter) = 0;
    virtual bool Init(const std::shared_ptr<indexlib::index::MultiFieldIndexReader>& multiFieldIndexReader) = 0;

protected:
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
};

} // namespace indexlibv2::table
