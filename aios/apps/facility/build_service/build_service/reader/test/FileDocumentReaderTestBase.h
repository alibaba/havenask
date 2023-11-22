#pragma once

#include "build_service/common_define.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class FileDocumentReaderTestBase : public BUILD_SERVICE_TESTBASE
{
public:
    void testReadEmptyDoc();
    void testReadEmptyFile();
    void testReadOneDoc();
    void testReadMultiDocs();
    void testReadMultiDocsWithOffset();
    void testReadOffsetPastEof();
    void testSeek();

public:
    void makeOneDocumentFile(const std::string& fileName, uint32_t docCount, uint32_t docContentPrefixLen,
                             bool endWithSep, std::vector<std::string>& docStringVect,
                             std::vector<int64_t>& offsetVect);

protected:
    void makeDocumentFile(uint32_t docCount, uint32_t docContentPrefixLen, bool endWithSep, bool gzip,
                          std::vector<std::string>& docStringVect, std::vector<int64_t>& offsetVect);
    void internalCheckReadDoc(bool endWithSep, int64_t offset, int32_t startDocCount,
                              std::vector<std::string>& docStringVect, std::vector<int64_t>& offsetVect);
    void checkReadDoc(uint32_t docCount, uint32_t docSize);
    void checkReadDocWithOffset(uint32_t docCount, uint32_t docSize);

private:
    std::string makeOneDocument(uint32_t len);

protected:
    int32_t _bufferSize;
    std::string _docPrefix;
    std::string _docSuffix;
    std::string _fileName;
    std::string _baseDir;
};

}} // namespace build_service::reader
