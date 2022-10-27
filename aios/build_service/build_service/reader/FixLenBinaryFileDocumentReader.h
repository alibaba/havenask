#ifndef ISEARCH_BS_FIXLENBINARYFILEDOCUMENTREADER_H
#define ISEARCH_BS_FIXLENBINARYFILEDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FileDocumentReader.h"

namespace build_service {
namespace reader {

class FixLenBinaryFileDocumentReader : public FileDocumentReader
{
public:
    static const uint32_t READER_DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

public:
    FixLenBinaryFileDocumentReader(size_t fixLen,
                                   uint32_t bufferSize = READER_DEFAULT_BUFFER_SIZE);
    ~FixLenBinaryFileDocumentReader();
private:
    FixLenBinaryFileDocumentReader(const FixLenBinaryFileDocumentReader &);
    FixLenBinaryFileDocumentReader& operator=(const FixLenBinaryFileDocumentReader &);

protected:
    bool doRead(std::string& docStr) override;

private:
    size_t _fixLen;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FixLenBinaryFileDocumentReader);

}
}

#endif //ISEARCH_BS_FIXLENBINARYFILEDOCUMENTREADER_H
