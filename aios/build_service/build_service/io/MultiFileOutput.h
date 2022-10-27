#ifndef ISEARCH_BS_MULTI_FILE_OUTPUT_H
#define ISEARCH_BS_MULTI_FILE_OUTPUT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/io/IODefine.h"
#include "build_service/task_base/TaskFactory.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/io/IODefine.h"
#include <indexlib/storage/buffered_file_wrapper.h>

namespace build_service {
namespace io {

class MultiFileOutput : public Output {
public:
    static const std::string TEMP_MULTI_FILE_SUFFIX;

private:
    static constexpr uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;  // 2M
public:
    MultiFileOutput(const build_service::config::TaskOutputConfig& outputConfig);
    ~MultiFileOutput();
    MultiFileOutput(const MultiFileOutput&) = delete;
    MultiFileOutput& operator=(const MultiFileOutput&) = delete;

public:
    std::string getType() const override { return build_service::io::MULTI_FILE; }
    bool init(const build_service::KeyValueMap& initParams) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;

private:
    bool switchFile();
    IE_NAMESPACE(storage)::BufferedFileWrapper* createFile(
        const build_service::KeyValueMap& params);
    void close();

private:
    std::unique_ptr<IE_NAMESPACE(storage)::BufferedFileWrapper> _file;
    std::string _destDirectory;
    std::string _filePrefix;
    std::string _currentTempFileName;
    std::string _currentTargetFileName;
    int64_t _currentFileId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiFileOutput);

class MultiFileOutputCreator : public OutputCreator {
public:
    bool init(const build_service::config::TaskOutputConfig& outputConfig) override {
        _taskOutputConfig = outputConfig;
        return true;
    }
    OutputPtr create(const build_service::KeyValueMap& params) override {
        OutputPtr output(new MultiFileOutput(_taskOutputConfig));
        if (output->init(params)) {
            return output;
        }
        return OutputPtr();
    }

private:
    config::TaskOutputConfig _taskOutputConfig;
};

BS_TYPEDEF_PTR(MultiFileOutputCreator);

}  // namespace io
}  // namespace build_service

#endif
