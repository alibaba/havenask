#include "build_service/reader/FileReader.h"
#include <fslib/fslib.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, FileReader);

FileReader::FileReader() {
    _offset = 0;
}

FileReader::~FileReader() {
}

bool FileReader::init(const string& fileName, int64_t offset) {
    FileMeta fileMeta;
    if (FileSystem::getFileMeta(fileName, fileMeta) != EC_OK){
        string errorMsg = "get file meta failed! fileName:[" + fileName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _fileTotalSize = fileMeta.fileLength;
    if (_fileTotalSize < offset) {
        stringstream ss;
        ss << "failed to open file[" << fileName << "] with offset[" << offset << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _file.reset(FileSystem::openFile(fileName, READ));
    _offset = offset;
    return true;
}

bool FileReader::get(char* output, uint32_t size, uint32_t& sizeUsed) {
    sizeUsed = 0;
    ssize_t bytesReadOnce =_file->pread(output, size, _offset);
    if (bytesReadOnce == -1) {
        string errorMsg = "read error: " + FileSystem::getErrorString(_file->getLastError());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    } else if (bytesReadOnce == 0) {
        return false;  // end of file
    }
    sizeUsed = (uint32_t)bytesReadOnce;
    _offset += bytesReadOnce;
    return true;
}

bool FileReader::good() {
    if (_file->isOpened()) {
        return true;
    }
    return false;
}

bool FileReader::isEof() {
    return _offset >= _fileTotalSize;
}

bool FileReader::seek(int64_t offset) {
    if (offset > _fileTotalSize) {
        BS_LOG(ERROR, "seek %ld > total size %ld", offset, _fileTotalSize);
        return false;
    }
    _offset = offset;
    return true;
}

}
}
