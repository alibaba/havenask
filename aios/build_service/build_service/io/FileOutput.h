#ifndef ISEARCH_BS_FILEOUTPUT_H
#define ISEARCH_BS_FILEOUTPUT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/io/IODefine.h"
#include "build_service/config/TaskOutputConfig.h"
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/storage/buffered_file_wrapper.h>

namespace build_service {
namespace io {

class FileOutput : public Output {
public:
    FileOutput(const config::TaskOutputConfig& outputConfig);
    ~FileOutput();
private:
    FileOutput(const FileOutput &) = delete;
    FileOutput& operator=(const FileOutput &) = delete;

public:
    std::string getType() const override { return io::FILE; }
public:
    bool init(const KeyValueMap& initParams) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;
    void close();

private:
    void write(const void* buffer, size_t length);

private:
    static constexpr uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; // 2M

private:
    std::unique_ptr<IE_NAMESPACE(storage)::BufferedFileWrapper> _file;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileOutput);

class FileOutputCreator : public OutputCreator
{
public:
    bool init(const config::TaskOutputConfig& outputConfig) override {
        _taskOutputConfig = outputConfig;
        return true;
    }
    
    OutputPtr create(const KeyValueMap& params) override {
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

}
}

#endif //ISEARCH_BS_FILEOUTPUT_H
