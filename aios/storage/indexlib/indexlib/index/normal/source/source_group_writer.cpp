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
#include "indexlib/index/normal/source/source_group_writer.h"

#include "autil/ConstString.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/source/source_define.h"
#include "indexlib/index/normal/source/source_writer.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;

using autil::StringView;
using indexlib::config::SourceGroupConfigPtr;
using indexlib::file_system::DirectoryPtr;
using indexlib::util::BuildResourceMetrics;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceGroupWriter);

SourceGroupWriter::SourceGroupWriter() {}

SourceGroupWriter::~SourceGroupWriter() {}

void SourceGroupWriter::Init(const SourceGroupConfigPtr& groupConfig, BuildResourceMetrics* buildResourceMetrics)
{
    mGroupConfig = groupConfig;
    mDataWriter.reset(new GroupFieldDataWriter(mGroupConfig->GetParameter().GetFileCompressConfig()));

    mDataWriter->Init(SOURCE_DATA_FILE_NAME, SOURCE_OFFSET_FILE_NAME,
                      VarLenDataParamHelper::MakeParamForSourceData(groupConfig), buildResourceMetrics);
}

void SourceGroupWriter::AddDocument(const StringView& data) { mDataWriter->AddDocument(data); }

void SourceGroupWriter::Dump(const DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool,
                             const string& temperatureLayer)
{
    mDataWriter->Dump(directory, dumpPool, temperatureLayer);
}
}} // namespace indexlib::index
