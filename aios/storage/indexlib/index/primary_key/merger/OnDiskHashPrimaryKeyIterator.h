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
#include "indexlib/index/primary_key/PrimaryKeyIterator.h"

namespace indexlibv2::index {

template <typename Key>
class OnDiskHashPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
public:
    OnDiskHashPrimaryKeyIterator() {};
    ~OnDiskHashPrimaryKeyIterator() {};

protected:
    std::pair<std::unique_ptr<PrimaryKeyLeafIterator<Key>>, docid64_t>
    GenerateSubIteratorPair(const SegmentDataAdapter::SegmentDataType& segment,
                            std::unique_ptr<PrimaryKeyLeafIterator<Key>> leafIterator) const override
    {
        return std::make_pair(std::move(leafIterator), segment._baseDocId);
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
