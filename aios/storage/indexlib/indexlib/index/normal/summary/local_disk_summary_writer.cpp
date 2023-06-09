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
#include "indexlib/index/normal/summary/local_disk_summary_writer.h"

#include <sstream>

#include "autil/ConstString.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, LocalDiskSummaryWriter);

void LocalDiskSummaryWriter::AddDocument(const StringView& data) { mDataWriter->AddDocument(data); }

void LocalDiskSummaryWriter::Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool,
                                  const std::string& temperatureLayer)
{
    mDataWriter->Dump(directory, dumpPool, temperatureLayer);
}

void LocalDiskSummaryWriter::Init(const SummaryGroupConfigPtr& summaryGroupConfig,
                                  BuildResourceMetrics* buildResourceMetrics)
{
    mSummaryGroupConfig = summaryGroupConfig;
    mDataWriter.reset(
        new GroupFieldDataWriter(mSummaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfig()));
    mDataWriter->Init(SUMMARY_DATA_FILE_NAME, SUMMARY_OFFSET_FILE_NAME,
                      VarLenDataParamHelper::MakeParamForSummary(mSummaryGroupConfig), buildResourceMetrics);
}

const SummarySegmentReaderPtr LocalDiskSummaryWriter::CreateInMemSegmentReader()
{
    return InMemSummarySegmentReaderPtr(
        new InMemSummarySegmentReader(mSummaryGroupConfig, mDataWriter->GetDataAccessor()));
}
}} // namespace indexlib::index
