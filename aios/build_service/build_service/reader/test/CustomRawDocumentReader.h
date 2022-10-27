#ifndef ISEARCH_CUSTOMRAWDOCUMENTREADER_H
#define ISEARCH_CUSTOMRAWDOCUMENTREADER_H

#include <build_service/reader/RawDocumentReader.h>

namespace build_service {
namespace reader {

class CustomRawDocumentReader : public RawDocumentReader
{
public:
    CustomRawDocumentReader();
    CustomRawDocumentReader(const CustomRawDocumentReader &other);
    ~CustomRawDocumentReader();
public:
    /* override */ bool init(const ReaderInitParam &params);
    /* override */ bool seek(int64_t offset);
    /* override */ bool isEof() const;

protected:
    /* override */ RawDocumentReader::ErrorCode readDocStr(
            std::string &docStr, int64_t &offset, int64_t &timestamp);
private:
    struct RawDocDesc {
        std::string docId;
        int64_t offset;
    };
private:
    size_t _cursor;
    std::vector<RawDocDesc> _documents;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_CUSTOMRAWDOCUMENTREADER_H
