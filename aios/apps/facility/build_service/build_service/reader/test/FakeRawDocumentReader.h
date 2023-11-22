#pragma once

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"
#include "indexlib/document/document_factory_wrapper.h"

namespace build_service { namespace reader {

class FakeRawDocumentReader : public RawDocumentReader
{
public:
    FakeRawDocumentReader();
    ~FakeRawDocumentReader();

private:
    FakeRawDocumentReader(const FakeRawDocumentReader&);
    FakeRawDocumentReader& operator=(const FakeRawDocumentReader&);

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;

protected:
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override;

public:
    // for test
    void addRawDocument(const std::string& docStr, int64_t offset, bool readError = false);
    void setLimit(bool hasLimit) { _hasLimit = hasLimit; }

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
    bool _hasLimit;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeRawDocumentReader);

class FakeDocumentFactory : public indexlib::document::DocumentFactory
{
public:
    indexlib::document::RawDocument* CreateRawDocument(const indexlib::config::IndexPartitionSchemaPtr& schema)
    {
        return nullptr;
    }
    indexlib::document::DocumentParser* CreateDocumentParser(const indexlib::config::IndexPartitionSchemaPtr& schema)
    {
        return nullptr;
    }
};

}} // namespace build_service::reader
