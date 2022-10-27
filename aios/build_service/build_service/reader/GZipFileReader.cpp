#include "build_service/reader/GZipFileReader.h"
#include "build_service/reader/FileReader.h"
#include <fslib/fslib.h>
#include <new>
using namespace std;
using namespace fslib;
using namespace fslib::fs;

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, GZipFileReader);

GZipFileReader::GZipFileReader(uint32_t bufferSize,
                               FileReaderBase* next)
    : _next(next), _bufferSize(bufferSize), _buffer(NULL),
      _bufferNow(0), _bufferEnd(0)
{
    if (_next == NULL) {
        _ownNextReader = true;
    } else {
        _ownNextReader = false;
    }

    _strm.zalloc = Z_NULL;
    _strm.zfree = Z_NULL;
    _strm.opaque = Z_NULL;
    _strm.avail_in = 0;
    _strm.next_in = Z_NULL;
}

GZipFileReader::~GZipFileReader() {
    if(_buffer) {
        delete[] _buffer;
        _buffer = NULL;
    }

    inflateEnd(&_strm);
    if (_ownNextReader && _next) {
        delete _next;
        _next = NULL;
    }
}

#pragma pack(push,1)
struct GZipHeader
{
    unsigned char magic1; // 31
    unsigned char magic2; // 139
    unsigned char method; // 8
    unsigned char flags;
    uint32_t modTime;
    unsigned char extraFlags;
    unsigned char os;
};
#pragma pack(pop)

bool GZipFileReader::init(const std::string& fileName, int64_t offset) {
    _buffer = new(nothrow) char[_bufferSize + 1];
    if(!_buffer) {
        return false;
    }

    if (_next == NULL) {
        _next = new(nothrow) FileReader;
        if (_next == NULL) {
            string errorMsg = "failed to new FileReader";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    if(!_next->init(fileName, 0)) {
        return false;
    }

    FileMeta fileMeta;
    if (FileSystem::getFileMeta(fileName, fileMeta) != EC_OK){
        string errorMsg = "get file meta failed! fileName:[" + fileName+ "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _fileTotalSize = fileMeta.fileLength;

    if (fileMeta.fileLength == 0) {
        string errorMsg = "gzip file[" + fileName + "] size == 0. ";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return true;
    }

    if (!readGzipHeader()) {
        return false;
    }

    if (offset != 0 && !skipToOffset(offset)) {
        stringstream ss;
        ss << "failed to skip to offset[" << offset << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GZipFileReader::readGzipHeader() {
    _bufferNow = 0;
    _bufferEnd = 0;
    int ret = inflateInit2(&_strm,-MAX_WBITS);
    if (ret != Z_OK) {
        string errorMsg = "gzip infalte init error";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // read header off
    GZipHeader gzip_header;
    uint32_t used = 0;
    if(!_next->get((char *)&gzip_header, sizeof(gzip_header), used)) {
        string errorMsg = "file header read failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if( (used < sizeof(gzip_header))||
       (gzip_header.magic1 != 31) ||
       (gzip_header.magic2 != 139) ||
       (gzip_header.method != Z_DEFLATED) ||
       (gzip_header.flags & 0xe0)) {
        string errorMsg = "gzip file header check failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if(gzip_header.flags & 4) { // extra field
        char buf[2];
        if(!_next->get(buf, sizeof(buf), used)) {
            string errorMsg = "file header read failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if( (used < sizeof(buf)) ) {
            string errorMsg = "extra field length read failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        unsigned int len = (unsigned)(buf[1]);
        len <<= 8;
        len += (unsigned)(buf[0]);
        while(len--) {
            char checkBuf[1];
            if(!_next->get(checkBuf, sizeof(checkBuf), used)) {
                string errorMsg = "file header read failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            if(used < sizeof(checkBuf)) {
                string errorMsg = "extra field data read failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            if(checkBuf[0] < 0) {
                break;
            }
        }
    }

    if(gzip_header.flags & 8) { // file name
        char checkBuf[1];
        do {
            if(!_next->get(checkBuf, sizeof(checkBuf), used)) {
                string errorMsg = "file header read failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            if(used < sizeof(checkBuf)) {
                string errorMsg = "file name read failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        } while (checkBuf[0] > 0);
    }

    if(gzip_header.flags & 16) { // comment
        char checkBuf[1];
        do {
            if(!_next->get(checkBuf, sizeof(checkBuf), used)) {
                string errorMsg = "file header read failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            if(used < sizeof(checkBuf)) {
                string errorMsg = "comment read failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        } while (checkBuf[0] > 0);
    }

    if(gzip_header.flags & 2) { // header crc
        char checkBuf[2];
        if(!_next->get(checkBuf, sizeof(checkBuf), used)) {
            string errorMsg = "file header read failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if(used < sizeof(checkBuf)) {
            string errorMsg = "header crc read failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    return true;
}

bool GZipFileReader::skipToOffset(int64_t offset) {
    int64_t totalRead = 0;
    uint32_t sizeRead = 0;
    char* tempBuff = new char[TEMP_BUFFER_SIZE];
    while (totalRead < offset) {
        uint32_t sizeToRead = (offset - totalRead) > TEMP_BUFFER_SIZE ?
                              TEMP_BUFFER_SIZE : (offset - totalRead);
        if (!get(tempBuff, sizeToRead, sizeRead)) {
            stringstream ss;
            ss << "failed to read data for length of " << sizeToRead;
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            delete []tempBuff;
            return false;
        }

        totalRead += sizeRead;
    }

    delete []tempBuff;
    return true;
}

bool GZipFileReader::reloadBuffer() {
    uint32_t used = 0;
    if(!_next->get(_buffer, _bufferSize, used)) {
        _bufferEnd = 0;
        _bufferNow = 0;
        return false;
    }
    _buffer[used] = 0;
    _bufferEnd = used;
    _bufferNow = 0;
    return true;
}

bool GZipFileReader::get(char* output, uint32_t size, uint32_t& sizeUsed) {
    assert(_next);
    assert(_buffer);

    sizeUsed = 0;

    _strm.avail_out = size;
    _strm.next_out = (Bytef *)output;

    do {
        if(_bufferNow >= _bufferEnd) {
            if(!reloadBuffer()) {
                break;
            }
        }
        _strm.avail_in = _bufferEnd - _bufferNow;
        _strm.next_in = (Bytef *)&_buffer[_bufferNow];

        int ret = inflate(&_strm, Z_NO_FLUSH);
        if((ret != Z_STREAM_END) && (ret != Z_OK)) {
            stringstream ss;
            ss << "infalte error(" << ret << ")";
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }

        _bufferNow = _bufferEnd - _strm.avail_in;
        if(ret == Z_STREAM_END) {
            // end and read all data off
            while(reloadBuffer());
            break;
        }
    } while (_strm.avail_out > 0);

    sizeUsed = size - _strm.avail_out;
    if(sizeUsed) {
        return true;
    }

    return false;
}

bool GZipFileReader::seek(int64_t offset) {
    BS_LOG(ERROR, "GZipFileReader do not support seek");
    return false;
}

}
}
