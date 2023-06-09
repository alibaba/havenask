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
#ifndef ISEARCH_BS_FILEOUTPUT_H
#define ISEARCH_BS_FILEOUTPUT_H

#include "build_service/common_define.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/io/IODefine.h"
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/util/Log.h"

namespace indexlib { namespace file_system {
class BufferedFileWriter;
}} // namespace indexlib::file_system

namespace build_service { namespace io {

class FileOutput : public Output
{
public:
    FileOutput(const config::TaskOutputConfig& outputConfig);
    ~FileOutput();

private:
    FileOutput(const FileOutput&) = delete;
    FileOutput& operator=(const FileOutput&) = delete;

public:
    std::string getType() const override { return io::FILE; }

public:
    bool init(const KeyValueMap& initParams) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;
    void close();
    size_t getLength() const;
    bool cloneFrom(const std::string& srcFileName, size_t fileLength);

private:
    void write(const void* buffer, size_t length);

private:
    static constexpr uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; // 2M

private:
    std::unique_ptr<indexlib::file_system::BufferedFileWriter> _file;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileOutput);

class FileOutputCreator : public OutputCreator
{
public:
    bool init(const config::TaskOutputConfig& outputConfig) override
    {
        _taskOutputConfig = outputConfig;
        return true;
    }

    OutputPtr create(const KeyValueMap& params) override
    {
        OutputPtr output(new FileOutput(_taskOutputConfig));
        if (output->init(params)) {
            return output;
        }
        return OutputPtr();
    }

private:
    config::TaskOutputConfig _taskOutputConfig;
};
BS_TYPEDEF_PTR(FileOutputCreator);

}} // namespace build_service::io

#endif // ISEARCH_BS_FILEOUTPUT_H
