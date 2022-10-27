#ifndef ISEARCH_BS_VARLENBINARYFILEDOCUMENTREADER_H
#define ISEARCH_BS_VARLENBINARYFILEDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FileDocumentReader.h"

namespace build_service {
namespace reader {

class VarLenBinaryFileDocumentReader : public FileDocumentReader
{
public:
    static const uint32_t READER_DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

public:
    VarLenBinaryFileDocumentReader(uint32_t bufferSize = READER_DEFAULT_BUFFER_SIZE);
    ~VarLenBinaryFileDocumentReader();
private:
    VarLenBinaryFileDocumentReader(const VarLenBinaryFileDocumentReader &);
    VarLenBinaryFileDocumentReader& operator=(const VarLenBinaryFileDocumentReader &);
    
protected:
    bool doRead(std::string& docStr) override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(VarLenBinaryFileDocumentReader);

}
}

#endif //ISEARCH_BS_VARLENBINARYFILEDOCUMENTREADER_H
