#include "build_service/reader/FixLenBinaryFileDocumentReader.h"

using namespace std;

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, FixLenBinaryFileDocumentReader);

FixLenBinaryFileDocumentReader::FixLenBinaryFileDocumentReader(
        size_t fixLen, uint32_t bufferSize)
    : FileDocumentReader(bufferSize)
    , _fixLen(fixLen)
{
}

FixLenBinaryFileDocumentReader::~FixLenBinaryFileDocumentReader() {}

bool FixLenBinaryFileDocumentReader::doRead(string& docStr)
{
    size_t targetFileOffset = (_fileOffset + _fixLen - 1) / _fixLen * _fixLen;
    size_t alignSize = targetFileOffset - _fileOffset;
    if (alignSize > 0) {
        string tmp;
        tmp.resize(alignSize);
        read((char*)tmp.data(), alignSize);
    }
    
    docStr.clear();
    docStr.resize(_fixLen);
    char* buffer = (char*)docStr.data();
    return read(buffer, _fixLen);
}

}
}
