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

namespace indexlibv2::index {
class AttributeConfig;
class AttributePatchMerger;
class SegmentUpdateBitmap;

class AttributePatchMergerFactory : private autil::NoCopyable
{
public:
    AttributePatchMergerFactory() = default;
    ~AttributePatchMergerFactory() = default;

public:
    static std::unique_ptr<AttributePatchMerger> Create(const std::shared_ptr<AttributeConfig>& attributeConfig,
                                                        const std::shared_ptr<SegmentUpdateBitmap>& updateBitmap);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
