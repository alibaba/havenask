#ifndef ISEARCH_BS_MULTIFILEDOCUMENTREADER_H
#define ISEARCH_BS_MULTIFILEDOCUMENTREADER_H

#include "build_service/util/Log.h"
#include "build_service/reader/FileDocumentReader.h"
#include "build_service/reader/CollectResult.h"

namespace build_service {
namespace reader {

class MultiFileDocumentReader {
public:
    MultiFileDocumentReader(const CollectResults &fileList);
    virtual ~MultiFileDocumentReader() {}
    
public:
    // init for format document by seperators
    bool initForFormatDocument(const std::string &docPrefix, const std::string &docSuffix);

    // init for binary document 
    bool initForFixLenBinaryDocument(size_t length);
    bool initForVarLenBinaryDocument();
    
    bool read(std::string &docStr, int64_t &locator);
    bool isEof();
    bool seek(int64_t locator);
    int64_t estimateLeftTime();
private:
    bool skipToFileId(int64_t fileId);
    bool skipToNextFile();
    void createDocumentReader();
protected:
    // virtual for mock
    virtual bool calculateTotalFileSize();
public:
    void appendLocator(int64_t &locator);
    static void transLocatorToFileOffset(
            int64_t locator, int64_t &fileIndex, int64_t &offset);
    static void transFileOffsetToLocator(
            int64_t fileIndex, int64_t offset, int64_t &locator);
private:
    static const int32_t FILE_INDEX_LOCATOR_BIT_COUNT;
    static const uint64_t MAX_LOCATOR_FILE_INDEX;
    static const int32_t OFFSET_LOCATOR_BIT_COUNT;
    static const uint64_t MAX_LOCATOR_OFFSET;
private:
    int32_t _fileCursor;
    CollectResults _fileList;
    FileDocumentReaderPtr _fileDocumentReaderPtr;
    int64_t _startTime;
    int64_t _processedFileSize;
    int64_t _totalFileSize;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiFileDocumentReader);

}
}

#endif //ISEARCH_BS_MULTIFILEDOCUMENTREADER_H
