#include "indexlib/document/raw_document/test/RawDocumentMaker.h"

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/Locator.h"

namespace indexlibv2::document {

const std::string RawDocumentMaker::CMD("cmd");
const std::string RawDocumentMaker::INGESTION_TIMESTAMP("_ingest_ts");
const std::string RawDocumentMaker::TIMESTAMP("ts");
const std::string RawDocumentMaker::LOCATOR("locator");
const std::string RawDocumentMaker::DOC_BATCH_SEP(";");
const std::string RawDocumentMaker::FIELD_SEP(",");
const std::string RawDocumentMaker::SPATIAL_FIELD_SEP("|");
const std::string RawDocumentMaker::KV_SEP("=");
const std::string RawDocumentMaker::CONCURRENT_IDX("concurrent_idx");

std::unique_ptr<RawDocument> RawDocumentMaker::Make(const std::string& docStr)
{
    auto fieldStrVec = autil::StringUtil::split(docStr, SPATIAL_FIELD_SEP);
    if (fieldStrVec.size() <= 1) {
        auto newStr = docStr;
        autil::StringUtil::replaceAll(newStr, "\\,", "\020");
        fieldStrVec = autil::StringUtil::split(newStr, FIELD_SEP);
    }
    auto doc = std::make_unique<DefaultRawDocument>();
    for (const auto& fieldStr : fieldStrVec) {
        auto kv = autil::StringUtil::split(fieldStr, KV_SEP, false);
        if (kv.size() != 2) {
            return nullptr;
        }
        autil::StringUtil::trim(kv[0]);
        autil::StringUtil::trim(kv[1]);
        size_t n = kv[1].size();
        // remove trailing ;
        if (n > 0 && kv[1].back() == ';') {
            kv[1] = kv[1].substr(0, n - 1);
        }
        // autil::StringUtil::replace(kv[1], RD_MULTI_VALUE_SEP, autil::MULTI_VALUE_DELIMITER);
        doc->setField(autil::StringView(kv[0]), autil::StringView(kv[1]));
        if (kv[0] == TIMESTAMP) {
            int64_t timestamp = 0;
            if (!autil::StringUtil::fromString(kv[1], timestamp)) {
                return nullptr;
            }
            auto docInfo = doc->GetDocInfo();
            doc->setDocTimestamp(timestamp);
            docInfo.timestamp = timestamp;
            doc->SetDocInfo(docInfo);
        } else if (kv[0] == LOCATOR) {
            auto strVec = autil::StringUtil::split(kv[1], ":");
            if (strVec.size() == 2) {
                int64_t src, offset;
                if (!autil::StringUtil::fromString(strVec[0], src) ||
                    !autil::StringUtil::fromString(strVec[1], offset)) {
                    return nullptr;
                }
                doc->SetLocator(framework::Locator(src, offset));
            } else if (strVec.size() == 1) {
                int64_t offset = 0;
                if (!autil::StringUtil::fromString(strVec[0], offset)) {
                    return nullptr;
                }
                doc->SetLocator(framework::Locator(0, offset));
            } else {
                assert(strVec.size() == 3);
                int64_t src, offset;
                uint32_t concurrentIdx;
                if (!autil::StringUtil::fromString(strVec[0], src) ||
                    !autil::StringUtil::fromString(strVec[1], offset) ||
                    !autil::StringUtil::fromString(strVec[2], concurrentIdx)) {
                    return nullptr;
                }
                doc->SetLocator(framework::Locator(src, {offset, concurrentIdx}));
            }
        } else if (kv[0] == INGESTION_TIMESTAMP) {
            int64_t ingestTimestamp = 0;
            if (!autil::StringUtil::fromString(kv[1], ingestTimestamp)) {
                return nullptr;
            }
            doc->SetIngestionTimestamp(ingestTimestamp);
        } else if (kv[0] == CMD || kv[0] == CMD_TAG) {
            doc->setField(autil::StringView(CMD_TAG), autil::StringView(kv[1]));
        } else if (kv[0] == CONCURRENT_IDX) {
            uint32_t concurrentIdx = 0;
            if (!autil::StringUtil::fromString(kv[1], concurrentIdx)) {
                return nullptr;
            }
            auto docInfo = doc->GetDocInfo();
            docInfo.concurrentIdx = concurrentIdx;
            doc->SetDocInfo(docInfo);
        }
    }
    return doc;
}

std::vector<std::shared_ptr<RawDocument>> RawDocumentMaker::MakeBatch(const std::string& docsStr)
{
    auto docStrVec = autil::StringUtil::split(docsStr, DOC_BATCH_SEP);
    std::vector<std::shared_ptr<RawDocument>> docs;
    docs.reserve(docStrVec.size());
    for (const auto& docStr : docStrVec) {
        if (docStr.empty()) {
            continue;
        }
        auto doc = Make(docStr);
        if (!doc) {
            return {};
        }
        docs.emplace_back(std::move(doc));
    }
    return docs;
}

} // namespace indexlibv2::document
