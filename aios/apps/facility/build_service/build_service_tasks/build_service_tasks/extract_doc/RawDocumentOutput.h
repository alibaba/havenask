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
#ifndef ISEARCH_BUILD_SERVICE_TASKS_RAW_DOCUMENAT_OUTPUT_H
#define ISEARCH_BUILD_SERVICE_TASKS_RAW_DOCUMENAT_OUTPUT_H

#include <unordered_map>

#include "autil/HashFuncFactory.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/io/IODefine.h"
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/task_base/TaskFactory.h"
#include "build_service/util/Log.h"

namespace build_service_tasks {

class RawDocumentOutput : public build_service::io::Output
{
private:
    using DocFields = std::unordered_map<std::string, std::string>;

public:
    static const std::string OUTPUT_TYPE;

public:
    RawDocumentOutput(const build_service::config::TaskOutputConfig& outputConfig);
    ~RawDocumentOutput() {};
    RawDocumentOutput(const RawDocumentOutput&) = delete;
    RawDocumentOutput& operator=(const RawDocumentOutput&) = delete;

public:
    std::string getType() const override { return OUTPUT_TYPE; }
    bool init(const build_service::KeyValueMap& params) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;

    const build_service::io::OutputPtr& getInnerOutput() { return _innerOutput; }

public:
    void TEST_setFactory(const build_service::task_base::TaskFactoryPtr& factory) { _factory = factory; }

private:
    std::string serializeHa3RawDoc(const DocFields& kvMap);

private:
    build_service::io::OutputPtr _innerOutput;
    autil::HashFunctionBasePtr _hashFunc;
    std::string _hashField;
    bool _isSwiftOutput;
    build_service::task_base::TaskFactoryPtr _factory;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocumentOutput);

class RawDocumentOutputCreator : public build_service::io::OutputCreator
{
public:
    bool init(const build_service::config::TaskOutputConfig& outputConfig) override
    {
        _taskOutputConfig = outputConfig;
        return true;
    }
    build_service::io::OutputPtr create(const build_service::KeyValueMap& params) override
    {
        build_service::io::OutputPtr output(new RawDocumentOutput(_taskOutputConfig));
        if (output->init(params)) {
            return output;
        }
        return build_service::io::OutputPtr();
    }

private:
    build_service::config::TaskOutputConfig _taskOutputConfig;
};

BS_TYPEDEF_PTR(RawDocumentOutputCreator);

} // namespace build_service_tasks

#endif
