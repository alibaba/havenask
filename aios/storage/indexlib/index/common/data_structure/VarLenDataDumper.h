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

#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"

namespace indexlibv2::index {

class VarLenDataDumper
{
public:
    VarLenDataDumper();
    ~VarLenDataDumper();

public:
    bool Init(VarLenDataAccessor* accessor, const VarLenDataParam& dumpParam);

    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const std::string& offsetFileName,
                const std::string& dataFileName,
                const std::shared_ptr<indexlib::index::FSWriterParamDecider>& fsWriterParamDecider,
                std::vector<docid_t>* newOrder, autil::mem_pool::PoolBase* dumpPool);

    uint32_t GetDataItemCount() const { return _dataItemCount; }
    uint64_t GetMaxItemLength() const { return _maxItemLen; }

private:
    Status DumpToWriter(VarLenDataWriter& writer, std::vector<docid_t>* newOrder);

private:
    VarLenDataAccessor* _accessor;
    VarLenDataParam _outputParam;
    uint32_t _dataItemCount;
    uint64_t _maxItemLen;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
