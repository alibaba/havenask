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
#include "indexlib/index/kv/VarLenKVMerger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/index/kv/DFSValueWriter.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KeyWriter.h"
#include "indexlib/index/kv/Record.h"
#include "indexlib/index/kv/ValueWriter.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, VarLenKVMerger);

VarLenKVMerger::VarLenKVMerger() {}

VarLenKVMerger::~VarLenKVMerger() {}

Status VarLenKVMerger::CreateValueWriter()
{
    indexlib::file_system::WriterOption writerOption;
    ValueWriter::FillWriterOption(*_indexConfig, writerOption);
    auto [s, fileWriter] = _targetDir->GetIDirectory()->CreateFileWriter(KV_VALUE_FILE_NAME, writerOption).StatusWith();
    if (!s.IsOK()) {
        return s;
    }
    _valueWriter.reset(new DFSValueWriter(fileWriter));
    return Status::OK();
}

Status VarLenKVMerger::PrepareMerge(const SegmentMergeInfos& segMergeInfos)
{
    auto s = KVMerger::PrepareMerge(segMergeInfos);
    if (!s.IsOK()) {
        return s;
    }
    return CreateValueWriter();
}

Status VarLenKVMerger::AddRecord(const Record& record)
{
    auto offset = _valueWriter->GetLength();
    auto s = _valueWriter->Write(record.value);
    if (s.IsOK()) {
        s = _keyWriter->AddSimple(record.key, offset, record.timestamp);
    }
    return s;
}

Status VarLenKVMerger::Dump()
{
    bool shortOffset = _indexConfig->GetIndexPreference().GetHashDictParam().HasEnableShortenOffset() &&
                       _valueWriter->GetLength() <=
                           _indexConfig->GetIndexPreference().GetHashDictParam().GetMaxValueSizeForShortOffset();
    auto s = _keyWriter->Dump(_targetDir, true, shortOffset);
    if (!s.IsOK()) {
        return s;
    }

    s = _valueWriter->Dump(_targetDir);
    if (!s.IsOK()) {
        return s;
    }

    KVFormatOptions formatOpts;
    formatOpts.SetShortOffset(shortOffset);
    return formatOpts.Store(_targetDir);
}

void VarLenKVMerger::FillSegmentMetrics(indexlib::framework::SegmentMetrics* segMetrics)
{
    SegmentStatistics stat;
    _keyWriter->FillStatistics(stat);
    _valueWriter->FillStatistics(stat);
    stat.totalMemoryUse = stat.keyMemoryUse + stat.valueMemoryUse;
    stat.keyValueSizeRatio = 1.0f * stat.keyMemoryUse / stat.totalMemoryUse;
    if (segMetrics != nullptr) {
        stat.Store(segMetrics, _indexConfig->GetIndexName());
    }
}

std::pair<int64_t, int64_t> VarLenKVMerger::EstimateMemoryUsage(const std::vector<SegmentStatistics>& statVec)
{
    auto [keyMemoryUsage, memoryUsage] = KVMerger::EstimateMemoryUsage(statVec);
    memoryUsage += indexlib::file_system::WriterOption::DEFAULT_COMPRESS_BUFFER_SIZE; // value

    return {keyMemoryUsage, memoryUsage};
}

} // namespace indexlibv2::index
