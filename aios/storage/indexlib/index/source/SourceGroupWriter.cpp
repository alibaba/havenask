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
#include "indexlib/index/source/SourceGroupWriter.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
using SourceGroupConfig = indexlib::config::SourceGroupConfig;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SourceGroupWriter);

SourceGroupWriter::SourceGroupWriter() {}

SourceGroupWriter::~SourceGroupWriter() {}

void SourceGroupWriter::Init(const std::shared_ptr<SourceGroupConfig>& groupConfig)
{
    _groupConfig = groupConfig;
    _dataWriter.reset(new GroupFieldDataWriter(_groupConfig->GetParameter().GetFileCompressConfigV2()));
    _dataWriter->Init(SOURCE_DATA_FILE_NAME, SOURCE_OFFSET_FILE_NAME,
                      VarLenDataParamHelper::MakeParamForSourceData(groupConfig));
}

void SourceGroupWriter::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    if (_dataWriter) {
        _dataWriter->UpdateMemUse(memUpdater);
    }
}

void SourceGroupWriter::AddDocument(const autil::StringView& data)
{
    if (_dataWriter) {
        _dataWriter->AddDocument(data);
    }
}

Status SourceGroupWriter::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                               autil::mem_pool::PoolBase* dumpPool,
                               const std::shared_ptr<framework::DumpParams>& params)
{
    return _dataWriter ? _dataWriter->Dump(directory, dumpPool, params)
                       : Status::InvalidArgs("no data writer for source group writer");
}

} // namespace indexlibv2::index
