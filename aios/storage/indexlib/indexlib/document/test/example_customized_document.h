#ifndef __INDEXLIB_EXAMPLE_CUSTOMIZED_DOCUMENT_H
#define __INDEXLIB_EXAMPLE_CUSTOMIZED_DOCUMENT_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_factory.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/indexlib.h"

namespace indexlib_example {
class MyRawDocumentParser : public indexlib::document::RawDocumentParser
{
public:
    MyRawDocumentParser();

    bool parse(const std::string& docString, indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return true;
    }
    bool parseMultiMessage(const std::vector<indexlib::document::RawDocumentParser::Message>& msgs,
                           indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return true;
    }
    std::string getId() const { return mId; }

private:
    std::string mId;
};

class MyDocument : public indexlib::document::Document
{
protected:
    void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const override;

    void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) override;

public:
    uint32_t GetDocBinaryVersion() const override;
    void SetDocId(docid_t docId) override;
    docid_t GetDocId() const override;
    size_t EstimateMemory() const override { return sizeof(*this); }

private:
    docid_t mDocId;
};

typedef std::shared_ptr<MyDocument> MyDocumentPtr;
class MyDocumentParser : public indexlib::document::DocumentParser
{
public:
    MyDocumentParser(const indexlib::config::IndexPartitionSchemaPtr& schema);
    ~MyDocumentParser();

public:
    bool DoInit() override;
    indexlib::document::DocumentPtr Parse(const indexlib::document::IndexlibExtendDocumentPtr& extendDoc) override;
    indexlib::document::DocumentPtr Parse(const autil::StringView& serializedData) override;
};

class MyDocumentFactory : public indexlib::document::DocumentFactory
{
public:
    MyDocumentFactory();
    ~MyDocumentFactory() {}

public:
    indexlib::document::RawDocument*
    CreateRawDocument(const indexlib::config::IndexPartitionSchemaPtr& schema) override;

    indexlib::document::DocumentParser*
    CreateDocumentParser(const indexlib::config::IndexPartitionSchemaPtr& schema) override;

    indexlib::document::RawDocumentParser*
    CreateRawDocumentParser(const indexlib::config::IndexPartitionSchemaPtr& schema,
                            const indexlib::document::DocumentInitParamPtr& initParam) override;

private:
    indexlib::document::KeyMapManagerPtr mHashKeyMapManager;
};

} // namespace indexlib_example

#endif //__INDEXLIB_EXAMPLE_CUSTOMIZED_DOCUMENT_H
