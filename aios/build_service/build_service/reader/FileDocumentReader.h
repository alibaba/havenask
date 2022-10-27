#ifndef ISEARCH_BS_FILEDOCUMENTREADER_H
#define ISEARCH_BS_FILEDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <fslib/fslib.h>
#include "build_service/reader/FileReaderBase.h"
#include "build_service/reader/Separator.h"

namespace build_service {
namespace reader {

class FileDocumentReader
{
public:
    FileDocumentReader(uint32_t bufferSize);
    virtual ~FileDocumentReader();

private:
    FileDocumentReader(const FileDocumentReader &);
    FileDocumentReader& operator=(const FileDocumentReader &);

public:
    virtual bool init(const std::string& fileName, int64_t offset);
    virtual bool read(std::string& docStr);

    virtual int64_t getFileOffset() const {
        return _fileOffset;
    }

    virtual bool isEof() const {
        return _isEof;
    }

    uint32_t getReadDocCount() const {
        return _readDocCount;
    }

    int64_t getFileSize() const {
        return _fReader ? _fReader->getFileLength() : 0;
    }

    int64_t getReadBytes() const {
        return _fReader ? _fReader->getReadBytes() : 0;
    }

    bool seek(int64_t offset);
protected:
    virtual bool doRead(std::string& docStr) = 0;
    
    bool fillBuffer();
    void readUntilPos(char *pos, uint32_t sepLen, std::string &docStr);
    void readUntilPos(char *pos, uint32_t sepLen, char* &buffer);

    bool read(char* buffer, size_t len);
    
    inline bool readVUInt32(uint32_t &ret)
    {
        uint8_t byte;
        if (!read((char*)&byte, sizeof(uint8_t))) {
            return false;
        }
        uint32_t value = byte & 0x7F;
        int shift = 7;
        while (byte & 0x80)
        {
            if (!read((char*)&byte, sizeof(uint8_t))) {
                return false;
            }
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        ret = value;
        return true;
    }
    
protected:
    char *_bufferCursor;
    char *_bufferEnd;
    char *_buffer;
    uint32_t _bufferSize;
    int64_t _fileOffset; // locator
    FileReaderBasePtr _fReader;     // file
    std::string _fileName;
    bool _isEof;
    bool _needReinit;
    uint32_t _readDocCount;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileDocumentReader);

}
}

#endif //ISEARCH_BS_FILEDOCUMENTREADER_H
