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
#include <vector>

#include "autil/Log.h"
#include "indexlib/index/aggregation/IValueIterator.h"

namespace indexlibv2::index {

class SequentialMultiSegmentValueIterator final : public IValueIterator
{
public:
    bool HasNext() const override;
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override;

    std::vector<std::unique_ptr<IValueIterator>> Steal();

public:
    static std::unique_ptr<IValueIterator> Create(std::vector<std::unique_ptr<IValueIterator>> segIters);

private:
    void Init(std::vector<std::unique_ptr<IValueIterator>> iters);

private:
    std::vector<std::unique_ptr<IValueIterator>> _iters;
    size_t _cursor = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
