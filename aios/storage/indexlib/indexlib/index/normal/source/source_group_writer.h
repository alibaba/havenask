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
#ifndef __INDEXLIB_SOURCE_GROUP_WRITER_H
#define __INDEXLIB_SOURCE_GROUP_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/index/normal/accessor/group_field_data_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index {

class SourceGroupWriter
{
public:
    SourceGroupWriter();
    ~SourceGroupWriter();

public:
    void Init(const config::SourceGroupConfigPtr& groupConfig, util::BuildResourceMetrics* buildResourceMetrics);

    // If any validation needs to be done, add a preprocess check instead of returning false here.
    void AddDocument(const autil::StringView& data);

    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool,
              const std::string& temperatureLayer);
    VarLenDataAccessor* GetDataAccessor() { return mDataWriter->GetDataAccessor(); }

private:
    config::SourceGroupConfigPtr mGroupConfig;
    GroupFieldDataWriterPtr mDataWriter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceGroupWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_SOURCE_GROUP_WRITER_H
