#pragma once

#include <memory>
#include <string>

#include "indexlib/document/RawDocument.h"

namespace indexlibv2::document {

class RawDocumentMaker
{
public:
    // format: a=b,c=d,...
    static std::unique_ptr<RawDocument> Make(const std::string& docStr);

    // format: a=b,c=d;
    //         e=f,x=y;
    static std::vector<std::shared_ptr<RawDocument>> MakeBatch(const std::string& docStr);

private:
    static const std::string CMD;
    static const std::string TIMESTAMP;
    static const std::string INGESTION_TIMESTAMP;
    static const std::string LOCATOR;
    static const std::string DOC_BATCH_SEP;
    static const std::string FIELD_SEP;
    static const std::string SPATIAL_FIELD_SEP;
    static const std::string KV_SEP;
    static constexpr char RD_MULTI_VALUE_SEP = ' ';
    static const std::string CONCURRENT_IDX;
};

} // namespace indexlibv2::document
