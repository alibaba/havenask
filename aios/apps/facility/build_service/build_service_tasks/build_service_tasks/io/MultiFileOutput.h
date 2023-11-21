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
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/io/IODefine.h"
#include "indexlib/file_system/archive/LogFile.h"

namespace build_service_tasks {

class MultiFileOutput : public build_service::io::Output
{
public:
    static const std::string TEMP_MULTI_FILE_SUFFIX;

private:
    static constexpr uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; // 2M
public:
    MultiFileOutput(const build_service::config::TaskOutputConfig& outputConfig);
    ~MultiFileOutput();
    MultiFileOutput(const MultiFileOutput&) = delete;
    MultiFileOutput& operator=(const MultiFileOutput&) = delete;

public:
    std::string getType() const override { return build_service_tasks::MULTI_FILE; }
    bool init(const build_service::KeyValueMap& initParams) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;

private:
    bool switchFile();
    indexlib::file_system::BufferedFileWriter* createFile(const build_service::KeyValueMap& params);
    void close();

private:
    std::unique_ptr<indexlib::file_system::BufferedFileWriter> _file;
    std::string _destDirectory;
    std::string _filePrefix;
    std::string _currentTempFileName;
    std::string _currentTargetFileName;
    int64_t _currentFileId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiFileOutput);

class MultiFileOutputCreator : public build_service::io::OutputCreator
{
public:
    bool init(const build_service::config::TaskOutputConfig& outputConfig) override
    {
        _taskOutputConfig = outputConfig;
        return true;
    }
    build_service::io::OutputPtr create(const build_service::KeyValueMap& params) override
    {
        build_service::io::OutputPtr output(new MultiFileOutput(_taskOutputConfig));
        if (output->init(params)) {
            return output;
        }
        return build_service::io::OutputPtr();
    }

private:
    build_service::config::TaskOutputConfig _taskOutputConfig;
};

BS_TYPEDEF_PTR(MultiFileOutputCreator);

} // namespace build_service_tasks
