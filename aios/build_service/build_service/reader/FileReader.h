#ifndef ISEARCH_BS_FILEREADER_H
#define ISEARCH_BS_FILEREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FileReaderBase.h"
#include <fslib/fslib.h>

namespace build_service {
namespace reader {

class FileReader : public FileReaderBase
{
public:
    FileReader();
    ~FileReader();
private:
    FileReader(const FileReader &);
    FileReader& operator=(const FileReader &);
public:
    bool init(const std::string& fileName, int64_t offset);
    bool get(char* output, uint32_t size, uint32_t& sizeUsed);
    bool good();
    bool isEof();
    int64_t getReadBytes() const {
        return _offset;
    }
    bool seek(int64_t offset) override;
private:
    fslib::fs::FilePtr _file;
    int64_t _offset;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileReader);

}
}

#endif //ISEARCH_BS_FILEREADER_H
