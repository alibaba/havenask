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
#include "gmock/gmock.h"
#include <memory>
#include <utility>

#include "indexlib/base/Status.h"
#include "indexlib/framework/TabletLoader.h"

class MemoryQuotaController;

namespace indexlibv2::framework {

class MockTabletLoader : public TabletLoader
{
public:
    MockTabletLoader() {}
    ~MockTabletLoader() {}

    MOCK_METHOD(Status, DoPreLoad, (const TabletData&, std::vector<std::shared_ptr<Segment>>, const Version&),
                (override));
    MOCK_METHOD((std::pair<Status, std::unique_ptr<TabletData>>), FinalLoad, (const TabletData&), (override));
    MOCK_METHOD((std::pair<Status, size_t>), EstimateMemUsed,
                (const std::shared_ptr<config::ITabletSchema>&, const std::vector<framework::Segment*>&), (override));
};
} // namespace indexlibv2::framework
