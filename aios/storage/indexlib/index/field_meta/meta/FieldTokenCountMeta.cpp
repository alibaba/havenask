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
#include "indexlib/index/field_meta/meta/FieldTokenCountMeta.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldTokenCountMeta);

FieldTokenCountMeta* FieldTokenCountMeta::Clone() { return new FieldTokenCountMeta(*this); }
Status FieldTokenCountMeta::Build(const FieldValueBatch& fieldValues)
{
    for (const auto& [fieldValueMeta, _, __] : fieldValues) {
        const auto& [___, tokenCount] = fieldValueMeta;
        _docCount++;
        _totalTokenCount += tokenCount;
        _avgFieldTokenCount.store(_totalTokenCount * 1.0 / _docCount);
    }
    return Status::OK();
}

Status FieldTokenCountMeta::Store(autil::mem_pool::PoolBase* dumpPool,
                                  const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    indexlib::file_system::WriterOption option;
    const std::string& fileName = FIELD_TOKEN_COUNT_META_FILE_NAME;
    auto [status, fileWriter] = indexDirectory->CreateFileWriter(fileName, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "faile to create file writer for [%s], ErrorInfo: [%s]. ", fileName.c_str(),
                  status.ToString().c_str());
        return status;
    }
    std::string fieldTokenCountInfo = ToJsonString(*this);
    status = fileWriter->Write(fieldTokenCountInfo.c_str(), fieldTokenCountInfo.size()).Status();
    RETURN_IF_STATUS_ERROR(status, "field token count info writer failed to file [%s]",
                           fileWriter->DebugString().c_str());
    status = fileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "field token count writer [%s] close failed", fileWriter->DebugString().c_str());
    return Status::OK();
}

Status FieldTokenCountMeta::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    std::string fieldMeta;
    auto status = directory
                      ->Load(FIELD_TOKEN_COUNT_META_FILE_NAME,
                             indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), fieldMeta)
                      .Status();
    RETURN_IF_STATUS_ERROR(status, "load field token count meta from [%s] failed",
                           directory->GetPhysicalPath("").c_str());
    FromJsonString(*this, fieldMeta);
    return Status::OK();
}

void FieldTokenCountMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("doc_count", _docCount, _docCount);
    json.Jsonize("total_token_count", _totalTokenCount, _totalTokenCount);

    if (json.GetMode() == FROM_JSON) {
        _avgFieldTokenCount.store(_docCount > 0 ? _totalTokenCount * 1.0 / _docCount : 0);
    }
}

Status FieldTokenCountMeta::Merge(const std::shared_ptr<IFieldMeta>& other)
{
    auto fieldTokenCountMeta = std::dynamic_pointer_cast<FieldTokenCountMeta>(other);
    if (!fieldTokenCountMeta) {
        assert(false);
        return Status::Corruption("cast field token count meta failed");
    }
    _docCount += fieldTokenCountMeta->_docCount;
    _totalTokenCount += fieldTokenCountMeta->_totalTokenCount;
    if (_docCount > 0) {
        _avgFieldTokenCount.store(_totalTokenCount * 1.0 / _docCount);
    }
    return Status::OK();
}

void FieldTokenCountMeta::GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                                    int64_t& dumpFileSize)
{
    currentMemUse = 0;
    dumpTmpMemUse = 0;
    dumpExpandMemUse = 0;
    dumpFileSize = 0;
    return;
}

double FieldTokenCountMeta::GetAvgFieldTokenCount() const { return _avgFieldTokenCount.load(); }

} // namespace indexlib::index
