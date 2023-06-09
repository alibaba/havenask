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
#include "indexlib/index/summary/LocalDiskSummaryMemIndexer.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/DumpParams.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/SummaryMemReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, LocalDiskSummaryMemIndexer);

void LocalDiskSummaryMemIndexer::AddDocument(const autil::StringView& data) { _dataWriter->AddDocument(data); }

Status LocalDiskSummaryMemIndexer::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                        autil::mem_pool::PoolBase* dumpPool,
                                        const std::shared_ptr<framework::DumpParams>& params)
{
    return _dataWriter->Dump(directory, dumpPool, params);
}

void LocalDiskSummaryMemIndexer::Init(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig)
{
    _summaryGroupConfig = summaryGroupConfig;
    _dataWriter.reset(
        new GroupFieldDataWriter(_summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfigV2()));
    _dataWriter->Init(SUMMARY_DATA_FILE_NAME, SUMMARY_OFFSET_FILE_NAME,
                      VarLenDataParamHelper::MakeParamForSummary(_summaryGroupConfig));
}

const std::shared_ptr<SummaryMemReader> LocalDiskSummaryMemIndexer::CreateInMemReader()
{
    return std::make_shared<SummaryMemReader>(_summaryGroupConfig, _dataWriter->GetDataAccessor());
}

void LocalDiskSummaryMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    return _dataWriter->UpdateMemUse(memUpdater);
}

} // namespace indexlibv2::index
