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

namespace indexlib::index {
class IInvertedIndexPatchIterator;
}
namespace indexlibv2::config {
class ITabletSchema;
}
namespace indexlibv2::framework {
class Segment;
}

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlib::index {
class InvertedIndexPatchIteratorCreator : private autil::NoCopyable
{
public:
    static std::shared_ptr<IInvertedIndexPatchIterator>
    Create(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
           const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments,
           const std::shared_ptr<indexlib::file_system::IDirectory>& patchExtraDir);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
