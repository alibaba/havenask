#pragma once

#include <memory>
#include <vector>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"

namespace indexlibv2::document {
class NormalDocument;
class IDocumentFactory;
class RawDocument;
class NormalDocumentParser;
class ExtendDocument;
class NormalExtendDocument;

class NormalDocumentMaker
{
public:
    static std::shared_ptr<NormalDocument> Make(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const std::string& docStr);
    static std::shared_ptr<NormalDocument> Make(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const std::shared_ptr<RawDocument>& rawDoc);

    static std::shared_ptr<IDocumentBatch> MakeBatch(const std::shared_ptr<config::ITabletSchema>& schema,
                                                     const std::string& docBatchStr);
    static std::shared_ptr<IDocumentBatch> MakeBatch(const std::shared_ptr<config::ITabletSchema>& schema,
                                                     const std::vector<std::shared_ptr<RawDocument>>& rawDocs);

    static void SetUDFSourceFields(const std::vector<std::string>& udfFieldNames);

private:
    static std::unique_ptr<IDocumentFactory> CreateDocumentFactory(const std::string& tableType);
    static std::unique_ptr<NormalDocumentParser>
    CreateDocumentParser(const std::shared_ptr<config::ITabletSchema>& schema);
    static Status InitExtendDoc(const std::shared_ptr<config::ITabletSchema>& schema, ExtendDocument* extendDoc);
    static std::vector<std::string> GetMultiField(const std::shared_ptr<config::ITabletSchema>& schema);
    static void ConvertMultiField(const std::vector<std::string>& multiFields,
                                  const std::shared_ptr<RawDocument>& rawDoc);
    static void ConvertModifyFields(const std::shared_ptr<config::ITabletSchema>& schema,
                                    NormalExtendDocument* extendDoc);
    static void InitSourceDocument(const std::shared_ptr<config::ITabletSchema>& schema, ExtendDocument* extendDoc);

private:
    static std::vector<std::string> _udfFieldNames;

private:
    inline static const std::string RESERVED_MODIFY_FIELDS = "modify_fields";
    inline static const std::string RESERVED_MODIFY_VALUES = "modify_values";
    inline static const std::string MODIFY_FIELDS_SEP = "#";
    inline static const std::string LAST_VALUE_PREFIX = "__last_value__";
};

} // namespace indexlibv2::document
