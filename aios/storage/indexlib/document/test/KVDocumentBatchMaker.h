#pragma once

#include <memory>
#include <vector>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"

namespace indexlibv2::document {

class IDocumentFactory;
class RawDocument;
class KVDocument;

class KVDocumentBatchMaker
{
public:
    static std::shared_ptr<IDocumentBatch> Make(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const std::string& docBatchStr);
    static std::vector<std::shared_ptr<IDocumentBatch>>
    MakeBatchVec(const std::shared_ptr<config::ITabletSchema>& schema, const std::string& docBatchStr);

    static std::shared_ptr<IDocumentBatch> Make(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const std::vector<std::shared_ptr<RawDocument>>& rawDocs);
    static std::vector<std::shared_ptr<IDocumentBatch>>
    MakeBatchVec(const std::shared_ptr<config::ITabletSchema>& schema,
                 const std::vector<std::shared_ptr<RawDocument>>& rawDocs);

private:
    static std::unique_ptr<IDocumentFactory> CreateDocumentFactory(const std::string& tableType);
    static std::vector<std::string> GetMultiField(const std::shared_ptr<config::ITabletSchema>& schema);
    static void ConvertMultiField(const std::vector<std::string>& multiFields,
                                  const std::shared_ptr<RawDocument>& rawDoc);
};

} // namespace indexlibv2::document
