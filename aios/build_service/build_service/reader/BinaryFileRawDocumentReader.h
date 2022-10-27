#ifndef ISEARCH_BS_BINARYFILERAWDOCUMENTREADER_H
#define ISEARCH_BS_BINARYFILERAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FileRawDocumentReader.h"

namespace build_service {
namespace reader {

class BinaryFileRawDocumentReader : public FileRawDocumentReader
{
public:
    BinaryFileRawDocumentReader(bool isFixLen);
    ~BinaryFileRawDocumentReader();
private:
    BinaryFileRawDocumentReader(const BinaryFileRawDocumentReader &);
    BinaryFileRawDocumentReader& operator=(const BinaryFileRawDocumentReader &);

protected:
    MultiFileDocumentReader *createMultiFileDocumentReader(
            const CollectResults &collectResult, const KeyValueMap &kvMap) override;

private:
    bool _isFixLen;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BinaryFileRawDocumentReader);

}
}

#endif //ISEARCH_BS_BINARYFILERAWDOCUMENTREADER_H
