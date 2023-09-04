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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {
class PatchIterator;
class PatchIteratorCreator : private autil::NoCopyable
{
public:
    PatchIteratorCreator() = default;
    ~PatchIteratorCreator() = default;

public:
    static std::unique_ptr<PatchIterator>
    Create(const std::shared_ptr<config::ITabletSchema>& schema,
           const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& segments);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
