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
#include "gmock/gmock.h"
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/IDiskIndexer.h"

namespace indexlibv2::config {
class IIndexConfig;
}
namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {

class MockDiskIndexer : public IDiskIndexer
{
public:
    MockDiskIndexer() = default;
    ~MockDiskIndexer() = default;
    MOCK_METHOD(Status, Open,
                (const std::shared_ptr<config::IIndexConfig>&,
                 const std::shared_ptr<indexlib::file_system::IDirectory>&),
                (override));
    MOCK_METHOD(size_t, EvaluateCurrentMemUsed, (), (override));
    MOCK_METHOD(size_t, EstimateMemUsed,
                (const std::shared_ptr<config::IIndexConfig>& indexConfig,
                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory),
                (override));
};

} // namespace indexlibv2::index
