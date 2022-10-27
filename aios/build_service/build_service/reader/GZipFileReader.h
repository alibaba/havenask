#ifndef ISEARCH_BS_GZIPFILEREADER_H
#define ISEARCH_BS_GZIPFILEREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FileReaderBase.h"
#include <zlib.h>

namespace build_service {
namespace reader {

class GZipFileReader : public FileReaderBase
{
public:
    GZipFileReader(uint32_t bufferSize, FileReaderBase* next = NULL);
    ~GZipFileReader();

private:
    GZipFileReader(const GZipFileReader &);
    GZipFileReader& operator=(const GZipFileReader &);

public:
    bool init(const std::string& fileName, int64_t offset);
    bool get(char* output, uint32_t size, uint32_t& sizeUsed);

    bool good() {return _next->good();}
    bool isEof() {
        return _next->isEof() && _bufferNow >= _bufferEnd;
    }
    int64_t getReadBytes() const {
        return _next == NULL ? 0 : _next->getReadBytes();
    }
    bool seek(int64_t offset) override;

private:
    bool reloadBuffer();
    bool readGzipHeader();
    bool skipToOffset(int64_t offset);

private:
    bool _ownNextReader;
    FileReaderBase* _next;
    uint32_t _bufferSize;
    char* _buffer;
    uint32_t _bufferNow;
    uint32_t _bufferEnd;

    z_stream _strm;

private:
    static const uint32_t TEMP_BUFFER_SIZE = 1024 * 1024 * 10; //10 M

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GZipFileReader);

}
}

#endif //ISEARCH_BS_GZIPFILEREADER_H
