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
#include <stdint.h>
#include <string>

#include "autil/legacy/any.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/io/IODefine.h"
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "swift/client/SwiftWriter.h"
#include "swift/common/MessageInfo.h"

namespace build_service { namespace io {

class SwiftOutput : public Output
{
public:
    SwiftOutput(const config::TaskOutputConfig& outputConfig);
    ~SwiftOutput();

private:
    SwiftOutput(const SwiftOutput&);
    SwiftOutput& operator=(const SwiftOutput&);

public:
    std::string getType() const override { return SWIFT; }

public:
    bool init(const KeyValueMap& params) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;

public:
    virtual bool write(swift::common::MessageInfo& msg);

public:
    int64_t getCommittedCheckpointId() const { return _swiftWriter->getCommittedCheckpointId(); }

private:
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::unique_ptr<swift::client::SwiftWriter> _swiftWriter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftOutput);
class SwiftOutputCreator : public OutputCreator
{
public:
    bool init(const config::TaskOutputConfig& outputConfig) override
    {
        _taskOutputConfig = outputConfig;
        return true;
    }

    OutputPtr create(const KeyValueMap& params) override
    {
        OutputPtr output(new SwiftOutput(_taskOutputConfig));
        if (output->init(params)) {
            return output;
        }
        return OutputPtr();
    }

private:
    config::TaskOutputConfig _taskOutputConfig;
};
BS_TYPEDEF_PTR(SwiftOutputCreator);

}} // namespace build_service::io
