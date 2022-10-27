#ifndef ISEARCH_BS_FAKERAWDOCUMENTREADER_H
#define ISEARCH_BS_FAKERAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include <indexlib/document/document_factory_wrapper.h>

namespace build_service {
namespace reader {

class FakeRawDocumentReader : public RawDocumentReader
{
public:
    FakeRawDocumentReader();
    ~FakeRawDocumentReader();
private:
    FakeRawDocumentReader(const FakeRawDocumentReader &);
    FakeRawDocumentReader& operator=(const FakeRawDocumentReader &);
public:
    /* override */ bool init(const ReaderInitParam &params);
    /* override */ bool seek(int64_t offset);
    /* override */ bool isEof() const;
    /* override */ int64_t getFreshness();
protected:
    /* override */ ErrorCode readDocStr(std::string &docStr, int64_t &offset, int64_t &timestamp);
public:
    // for test
    void addRawDocument(const std::string &docStr,
                        int64_t offset,
                        bool readError = false);
private:
    struct RawDocDesc {
        std::string docStr;
        int64_t offset;
        bool readError;
    };
private:
    size_t _cursor;
    std::vector<RawDocDesc> _documents;
    bool _resetParser;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeRawDocumentReader);


class FakeDocumentFactory : public IE_NAMESPACE(document)::DocumentFactory
{
public:
    IE_NAMESPACE(document)::RawDocument* CreateRawDocument(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema)
    {
        return nullptr;
    }
    IE_NAMESPACE(document)::DocumentParser* CreateDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema)
    {
        return nullptr;
    }
};

}
}

#endif //ISEARCH_BS_FAKERAWDOCUMENTREADER_H
