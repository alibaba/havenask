#include "build_service/reader/VarLenBinaryFileDocumentReader.h"
#include "build_service/reader/VarLenBinaryDocumentEncoder.h"

using namespace std;

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, VarLenBinaryFileDocumentReader);

VarLenBinaryFileDocumentReader::VarLenBinaryFileDocumentReader(uint32_t bufferSize)
    : FileDocumentReader(bufferSize)
{
}

VarLenBinaryFileDocumentReader::~VarLenBinaryFileDocumentReader() {
}

bool VarLenBinaryFileDocumentReader::doRead(string& docStr)
{
    docStr.clear();
    uint32_t magicNum = 0;
    if (!read((char*)&magicNum, sizeof(uint32_t))) {
        return false;
    }

    if (magicNum != VarLenBinaryDocumentEncoder::MAGIC_HEADER) {
        BS_LOG(ERROR, "invalid file offset [%ld] to read var length binary raw document, "
               "will read current file from beginning", _fileOffset - sizeof(uint32_t));
        if (!init(_fileName, 0)) {
            return false;
        }
        if (!read((char*)&magicNum, sizeof(uint32_t))) {
            return false;
        }
        if (magicNum != VarLenBinaryDocumentEncoder::MAGIC_HEADER) {
            BS_LOG(ERROR, "not match doc magic number!");
            return false;
        }
    }
    
    uint32_t len = 0;
    if (!readVUInt32(len)) {
        return false;
    }
    docStr.resize(len);
    return read((char*)docStr.data(), len);
}

}
}
