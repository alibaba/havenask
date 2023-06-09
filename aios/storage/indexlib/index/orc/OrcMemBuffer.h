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
#include "indexlib/base/Status.h"

namespace orc {
struct ColumnVectorBatch;
struct StructVectorBatch;
class Writer;
} // namespace orc

namespace indexlib::document {
class AttributeDocument;
}

namespace indexlibv2::config {
class OrcConfig;
}

namespace indexlibv2::index {

class ColumnBuffer;

class OrcMemBuffer : private autil::NoCopyable
{
public:
    OrcMemBuffer(uint64_t numRowsPerBatch);
    ~OrcMemBuffer();

    Status Init(const indexlibv2::config::OrcConfig& config, orc::Writer* writer);
    void Append(indexlib::document::AttributeDocument* doc);
    bool IsFull();
    void Seal();
    void Reset();

    uint64_t Size() { return _rowIdx; }
    orc::ColumnVectorBatch* GetColumnVectorBatch() { return _batchBase.get(); }

private:
    std::unique_ptr<orc::ColumnVectorBatch> _batchBase;
    orc::StructVectorBatch* _structBatch;
    std::vector<std::unique_ptr<ColumnBuffer>> _columns;

    uint64_t _numRowsPerBatch;
    uint64_t _rowIdx;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
