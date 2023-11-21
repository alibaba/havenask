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

#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/var_len_data_accessor.h"
#include "indexlib/index/data_structure/var_len_data_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenDataDumper
{
public:
    VarLenDataDumper();
    ~VarLenDataDumper();

public:
    bool Init(VarLenDataAccessor* accessor, const VarLenDataParam& dumpParam);

    void Dump(const file_system::DirectoryPtr& directory, const std::string& offsetFileName,
              const std::string& dataFileName, const FSWriterParamDeciderPtr& fsWriterParamDecider,
              std::vector<docid_t>* newOrder, autil::mem_pool::PoolBase* dumpPool);

    uint32_t GetDataItemCount() const { return mDataItemCount; }
    uint64_t GetMaxItemLength() const { return mMaxItemLen; }

private:
    void DumpToWriter(VarLenDataWriter& writer, std::vector<docid_t>* newOrder);

private:
    VarLenDataAccessor* mAccessor;
    VarLenDataParam mOutputParam;
    uint32_t mDataItemCount;
    uint64_t mMaxItemLen;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataDumper);
}} // namespace indexlib::index
