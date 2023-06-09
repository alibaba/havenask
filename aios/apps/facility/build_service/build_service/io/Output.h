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
#ifndef ISEARCH_BS_OUTPUT_H
#define ISEARCH_BS_OUTPUT_H

#include "autil/legacy/any.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace io {

class Output
{
public:
    Output(const config::TaskOutputConfig& outputConfig) : _outputConfig(outputConfig) {}
    virtual ~Output() {}

private:
    Output(const Output&);
    Output& operator=(const Output&);

public:
    virtual bool init(const KeyValueMap& params) = 0;
    virtual std::string getType() const = 0;
    virtual bool write(autil::legacy::Any& any) { return false; }
    virtual bool commit() { return false; }

protected:
    void mergeParams(const KeyValueMap& params)
    {
        for (const auto& kv : params) {
            _outputConfig.addParameters(kv.first, kv.second);
        }
    }

protected:
    config::TaskOutputConfig _outputConfig;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Output);

}} // namespace build_service::io

#endif // ISEARCH_BS_OUTPUT_H
