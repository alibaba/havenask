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

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"

namespace indexlibv2::framework {

class IndexTaskPlan : public autil::legacy::Jsonizable
{
public:
    IndexTaskPlan(const std::string& taskName, const std::string& taskType);
    ~IndexTaskPlan();

public:
    void Jsonize(JsonWrapper& json) override;
    void AddOperation(const IndexOperationDescription& opDesc) { _opDescs.push_back(opDesc); }
    const std::vector<IndexOperationDescription>& GetOpDescs() const { return _opDescs; }
    std::vector<IndexOperationDescription>& GetOpDescs() { return _opDescs; }
    void SetEndTaskOperation(const IndexOperationDescription& opDesc)
    {
        _endTaskOp = std::make_shared<IndexOperationDescription>(opDesc);
    }
    const IndexOperationDescription* GetEndTaskOpDesc() const { return _endTaskOp.get(); }

    const std::string& GetTaskName() const { return _taskName; }
    const std::string& GetTaskType() const { return _taskType; }

private:
    std::vector<IndexOperationDescription> _opDescs;
    std::shared_ptr<IndexOperationDescription> _endTaskOp;
    std::string _taskName;
    std::string _taskType;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
