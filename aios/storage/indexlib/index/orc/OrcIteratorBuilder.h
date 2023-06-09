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

#include "indexlib/index/orc/OrcReader.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool

namespace future_lite {
class Executor;
}

namespace indexlibv2::index {

class OrcIteratorBuilder
{
public:
    explicit OrcIteratorBuilder(OrcReader* reader);
    ~OrcIteratorBuilder();

public:
    OrcIteratorBuilder& SetAsync(bool async);
    OrcIteratorBuilder& SetBatchSize(uint32_t batchSize);
    OrcIteratorBuilder& SetPool(autil::mem_pool::Pool* pool);
    OrcIteratorBuilder& SetExecutor(future_lite::Executor* executor);
    OrcIteratorBuilder& SetSegmentIds(const std::vector<segmentid_t>& segments);
    OrcIteratorBuilder& SetFieldNames(const std::vector<std::string>& fieldNames);
    Status Build(std::unique_ptr<IOrcIterator>& orcIter);

private:
    OrcReader* _reader;
    ReaderOptions _opts;
    std::vector<segmentid_t> _segmentIds;
    std::vector<std::string> _fieldNames;
};

} // namespace indexlibv2::index
