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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"

namespace indexlib::index {

class OperationFieldInfo : public autil::legacy::Jsonizable
{
public:
    OperationFieldInfo();
    ~OperationFieldInfo();

public:
    static Status GetLatestOperationFieldInfo(const std::shared_ptr<file_system::IDirectory>& indexDir,
                                              std::shared_ptr<OperationFieldInfo>& operationFieldInfo);
    const std::string& GetFieldName(fieldid_t fieldId);
    void Init(const std::shared_ptr<OperationLogConfig>& opLogConfig);
    void DropField(fieldid_t fieldId);
    bool HasField(fieldid_t fieldId);
    uint32_t GetId() const { return _id; }
    void IncreaseId() { _id++; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Store(const std::shared_ptr<file_system::IDirectory>& indexDir);

public:
    void TEST_AddField(fieldid_t fieldId, const std::string& fieldName) { _fieldInfo[fieldId] = fieldName; }

private:
    static const std::string OPERATION_FIELD_INFO_FILE_NAME_PREFIX;
    static bool MatchPattern(const std::string& str, const std::string& prefix, char sep);
    static bool IsNotDropOperationFile(const std::string& fileName);
    static int32_t ExtractSuffixNumber(const std::string& name, const std::string& prefix);
    struct OperationFieldInfoFileComp {
        bool operator()(const std::string& lft, const std::string& rht)
        {
            int32_t lftVal =
                OperationFieldInfo::ExtractSuffixNumber(lft, OperationFieldInfo::OPERATION_FIELD_INFO_FILE_NAME_PREFIX);
            int32_t rhtVal =
                OperationFieldInfo::ExtractSuffixNumber(rht, OperationFieldInfo::OPERATION_FIELD_INFO_FILE_NAME_PREFIX);
            return lftVal < rhtVal;
        }
    };

private:
    std::map<fieldid_t, std::string> _fieldInfo;
    uint32_t _id;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
