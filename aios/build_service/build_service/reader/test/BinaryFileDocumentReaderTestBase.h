#ifndef ISEARCH_BS_BINARYFILEDOCUMENTREADERTESTBASE_H
#define ISEARCH_BS_BINARYFILEDOCUMENTREADERTESTBASE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include "build_service/reader/FileDocumentReader.h"
#include "build_service/reader/VarLenBinaryFileDocumentReader.h"
#include "build_service/reader/FixLenBinaryFileDocumentReader.h"

namespace build_service {
namespace reader {

class BinaryFileDocumentReaderTestBase : public BUILD_SERVICE_TESTBASE {
public:
    void testReadEmptyDoc();
    void testReadEmptyFile();
    void testReadOneDoc();
    void testReadMultiDocs();
    void testReadMultiDocsWithOffset();
    void testReadOffsetPastEof();
    
public:
    void makeDocumentFile(uint32_t docCount, uint32_t contentLen, bool gzip,
                          std::vector<std::string> &docStringVect,
                          std::vector<int64_t>& offsetVect);

    void makeVarLenBinaryDocumentFile(
            uint32_t docCount, uint32_t contentLen, bool gzip,
            std::vector<std::string> &docStringVect, std::vector<int64_t>& offsetVect);

    void makeFixLenBinaryDocumentFile(
            uint32_t docCount, uint32_t contentLen, bool gzip,
            std::vector<std::string> &docStringVect, std::vector<int64_t>& offsetVect);

protected:
    void internalCheckReadDoc(int64_t offset, int32_t startDocCount,
                              std::vector<std::string>& docStringVect,
                              std::vector<int64_t>& offsetVect);
    void checkReadDoc(uint32_t docCount, uint32_t docSize);
    void checkReadDocWithOffset(uint32_t docCount, uint32_t docSize);

    void makeOneFixLenDocumentFile(
            const std::string& fileName, uint32_t docCount, uint32_t contentLen, 
            std::vector<std::string>& docStringVect, std::vector<int64_t>& offsetVect);

    void makeOneVarLenDocumentFile(
            const std::string& fileName, uint32_t docCount, uint32_t contentLen, 
            std::vector<std::string>& docStringVect, std::vector<int64_t>& offsetVect);

    std::string makeOneDocument(uint32_t len, uint32_t idx);
    std::string makeOneDocument(uint32_t len);
    FileDocumentReaderPtr createFileDocumentReader();
    
protected:
    int32_t _bufferSize;
    std::string _fileName;
    std::string _baseDir;
    size_t _fixLen;
    bool _isFixLen;
};

}
}

#endif //ISEARCH_BS_BINARYFILEDOCUMENTREADERTESTBASE_H
