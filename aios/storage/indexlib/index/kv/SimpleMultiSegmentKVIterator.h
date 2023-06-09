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
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/MultiSegmentKVIterator.h"

namespace indexlibv2::index {

class SimpleMultiSegmentKVIterator final : public MultiSegmentKVIterator
{
public:
    SimpleMultiSegmentKVIterator(schemaid_t targetSchemaId, std::shared_ptr<config::KVIndexConfig> indexConfig,
                                 std::shared_ptr<AdapterIgnoreFieldCalculator> ignoreFieldCalculator,
                                 std::vector<std::shared_ptr<framework::Segment>> segments);
    ~SimpleMultiSegmentKVIterator();

public:
    Status Init() override;

    bool HasNext() const override;
    Status Next(autil::mem_pool::Pool* pool, Record& record) override;
    std::pair<offset_t, offset_t> GetOffset() const override;
    Status Seek(offset_t segmentIndex, offset_t offset) override;
    void Reset() override;

public:
    Status MoveToSegment(int32_t idx);

private:
    std::unique_ptr<IKVIterator> _curIter;
    int32_t _currentIndex;
};

} // namespace indexlibv2::index
