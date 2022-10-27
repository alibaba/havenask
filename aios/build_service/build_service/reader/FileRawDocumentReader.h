#ifndef ISEARCH_BS_FILERAWDOCUMENTREADER_H
#define ISEARCH_BS_FILERAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/CollectResult.h"

namespace build_service {
namespace reader {

class MultiFileDocumentReader;

class FileRawDocumentReader : public RawDocumentReader
{
public:
    FileRawDocumentReader();
    virtual ~FileRawDocumentReader();
private:
    FileRawDocumentReader(const FileRawDocumentReader &);
    FileRawDocumentReader& operator=(const FileRawDocumentReader &);
public:
    /* override */ bool init(const ReaderInitParam &params);
    /* override */ bool seek(int64_t offset);
    /* override */ bool isEof() const;
protected:
    /* override */ ErrorCode readDocStr(std::string &docStr, int64_t &offset, int64_t &timestamp);
    /* override */ int64_t getFreshness();
protected:
    virtual MultiFileDocumentReader *createMultiFileDocumentReader(
            const CollectResults &collectResult, const KeyValueMap &kvMap);
protected:
    MultiFileDocumentReader *_documentReader;
    IE_NAMESPACE(util)::StateCounterPtr _leftTimeCounter;
    
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_FILERAWDOCUMENTREADER_H
