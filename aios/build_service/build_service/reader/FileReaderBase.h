#ifndef ISEARCH_BS_FILEREADERBASE_H
#define ISEARCH_BS_FILEREADERBASE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace reader {

class FileReaderBase
{
public:
    enum FileType{
        FILE_TYPE = 0,
        GZIP_TYPE = 1,
    };
public:
    FileReaderBase()
        : _fileTotalSize(0)
    {}
    virtual ~FileReaderBase() {}

private:
    FileReaderBase(const FileReaderBase &);
    FileReaderBase& operator=(const FileReaderBase &);

public:
    static FileReaderBase* createFileReader(const std::string& fileName,
            uint32_t bufferSize);
    static FileType getFileType(const std::string& fileName);

public:
    virtual bool init(const std::string& fileName, int64_t offset) = 0;
    virtual bool get(char* output, uint32_t size, uint32_t& sizeUsed) = 0;
    virtual bool good() = 0;
    virtual bool isEof() = 0;
    virtual int64_t getReadBytes() const = 0;
    virtual bool seek(int64_t offset) = 0;
    int64_t getFileLength() const {
        return _fileTotalSize;
    };

protected:
    int64_t _fileTotalSize;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileReaderBase);

}
}

#endif //ISEARCH_BS_FILEREADERBASE_H
