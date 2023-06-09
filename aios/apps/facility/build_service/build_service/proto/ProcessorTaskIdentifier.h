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
#ifndef ISEARCH_BS_PROCESSORTASKIDENTIFIER_H
#define ISEARCH_BS_PROCESSORTASKIDENTIFIER_H

#include "build_service/common_define.h"
#include "build_service/proto/TaskIdentifier.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

class ProcessorTaskIdentifier : public TaskIdentifier
{
public:
    ProcessorTaskIdentifier();
    ~ProcessorTaskIdentifier();

private:
    ProcessorTaskIdentifier(const ProcessorTaskIdentifier&);
    ProcessorTaskIdentifier& operator=(const ProcessorTaskIdentifier&);

public:
    bool fromString(const std::string& content) override;
    std::string toString() const override;

    void setDataDescriptionId(uint32_t id) { _dataDescriptionId = id; }
    uint32_t getDataDescriptionId() const { return _dataDescriptionId; }

private:
    uint32_t _dataDescriptionId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorTaskIdentifier);

}} // namespace build_service::proto

#endif // ISEARCH_BS_PROCESSORTASKIDENTIFIER_H
