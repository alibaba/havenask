#ifndef ISEARCH_BS_FORMATFILEDOCUMENTREADER_H
#define ISEARCH_BS_FORMATFILEDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <fslib/fslib.h>
#include "build_service/reader/FileDocumentReader.h"
#include "build_service/reader/FileReaderBase.h"
#include "build_service/reader/Separator.h"

namespace build_service {
namespace reader {

class FormatFileDocumentReader : public FileDocumentReader
{
public:
    static const uint32_t READER_DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

public:
    FormatFileDocumentReader(const std::string &docPrefix, const std::string &docSuffix,
                             uint32_t bufferSize = READER_DEFAULT_BUFFER_SIZE);
    virtual ~FormatFileDocumentReader();

private:
    FormatFileDocumentReader(const FormatFileDocumentReader &);
    FormatFileDocumentReader& operator=(const FormatFileDocumentReader &);

protected:
    bool doRead(std::string& docStr) override;

private:
    bool findSeparator(const Separator &sep, std::string &docStr);

private:
    Separator _docPrefix;
    Separator _docSuffix;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FormatFileDocumentReader);

}
}

#endif //ISEARCH_BS_FORMATFILEDOCUMENTREADER_H
