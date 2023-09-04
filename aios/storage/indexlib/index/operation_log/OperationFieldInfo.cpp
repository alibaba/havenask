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
#include "indexlib/index/operation_log/OperationFieldInfo.h"

#include "indexlib/file_system/JsonUtil.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationFieldInfo);

const std::string OperationFieldInfo::OPERATION_FIELD_INFO_FILE_NAME_PREFIX = "operation_field_info";
OperationFieldInfo::OperationFieldInfo() : _id(0) {}

OperationFieldInfo::~OperationFieldInfo() {}

Status OperationFieldInfo::Store(const std::shared_ptr<file_system::IDirectory>& indexDir)
{
    std::string fileName = OPERATION_FIELD_INFO_FILE_NAME_PREFIX + "." + autil::StringUtil::toString(_id);
    return indexDir->Store(fileName, ToJsonString(*this), file_system::WriterOption::AtomicDump()).Status();
}

void OperationFieldInfo::Init(const std::shared_ptr<OperationLogConfig>& opLogConfig)
{
    auto fieldConfigs = opLogConfig->GetAllFieldConfigs();
    for (auto fieldConfig : fieldConfigs) {
        _fieldInfo[fieldConfig->GetFieldId()] = fieldConfig->GetFieldName();
    }
}

void OperationFieldInfo::DropField(fieldid_t fieldId) { _fieldInfo.erase(fieldId); }

bool OperationFieldInfo::HasField(fieldid_t fieldId) { return _fieldInfo.find(fieldId) != _fieldInfo.end(); }

void OperationFieldInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("id", _id);
    json.Jsonize("fields", _fieldInfo);
}

Status OperationFieldInfo::GetLatestOperationFieldInfo(const std::shared_ptr<file_system::IDirectory>& indexDir,
                                                       std::shared_ptr<OperationFieldInfo>& operationFieldInfo)
{
    fslib::FileList fileList;
    auto status = indexDir->ListDir("", file_system::ListOption(), fileList).Status();
    RETURN_IF_STATUS_ERROR(status, "list operation dir failed");
    fslib::FileList::iterator it = std::remove_if(fileList.begin(), fileList.end(), IsNotDropOperationFile);
    fileList.erase(it, fileList.end());
    if (fileList.size() == 0) {
        return Status::OK();
    }
    std::sort(fileList.begin(), fileList.end(), OperationFieldInfoFileComp());
    file_system::ReaderOption readOption(file_system::FSOT_MEM);
    std::string fileContent;
    status = indexDir->Load(fileList[fileList.size() - 1], readOption, fileContent).Status();
    RETURN_IF_STATUS_ERROR(status, "read drop operation file failed");
    operationFieldInfo.reset(new OperationFieldInfo);
    return file_system::JsonUtil::FromString(fileContent, operationFieldInfo.get()).Status();
}

const std::string& OperationFieldInfo::GetFieldName(fieldid_t fieldId)
{
    auto iter = _fieldInfo.find(fieldId);
    if (iter != _fieldInfo.end()) {
        return iter->second;
    }
    static std::string emptyStr;
    return emptyStr;
}

bool OperationFieldInfo::MatchPattern(const std::string& str, const std::string& prefix, char sep)
{
    if (str.find(prefix) != 0) {
        return false;
    }
    const size_t suffixPos = prefix.length() + sizeof(sep);
    if (str.length() < suffixPos || str[prefix.size()] != sep) {
        return false;
    }
    int32_t lftVal;
    return autil::StringUtil::strToInt32(str.data() + suffixPos, lftVal);
}

bool OperationFieldInfo::IsNotDropOperationFile(const std::string& fileName)
{
    return !MatchPattern(fileName, OPERATION_FIELD_INFO_FILE_NAME_PREFIX, '.');
}

int32_t OperationFieldInfo::ExtractSuffixNumber(const std::string& name, const std::string& prefix)
{
    int32_t number = -1;
    // format like version.${number}, prefix = version, 1 represent char '.'
    bool success = autil::StringUtil::strToInt32(name.data() + prefix.size() + 1, number);
    if (!success) {
        AUTIL_LOG(ERROR, "extract suffix number failed, name:[%s]", name.c_str());
        return number;
    }
    return number;
}

} // namespace indexlib::index
