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

#include <memory>

#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2 { namespace framework {
class IIndexMemoryReclaimer;
class MetricsManager;
}} // namespace indexlibv2::framework

namespace indexlib::util {
class SearchCachePartitionWrapper;
}

namespace indexlib::file_system {
class Directory;
}
namespace indexlibv2::framework {

struct ReadResource {
    MetricsManager* metricsManager = nullptr;
    std::shared_ptr<IIndexMemoryReclaimer> indexMemoryReclaimer;
    std::shared_ptr<indexlib::file_system::Directory> rootDirectory;
    std::shared_ptr<indexlib::util::SearchCachePartitionWrapper> searchCache;
};

} // namespace indexlibv2::framework
