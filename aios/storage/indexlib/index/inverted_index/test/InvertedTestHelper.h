#pragma once
#include <map>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/index/common/field_format/section_attribute/SectionMeta.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace indexlibv2::config {
class PackageIndexConfig;
class SingleFieldIndexConfig;
class ITabletSchema;
} // namespace indexlibv2::config

namespace indexlib::document {
class Field;
class Section;
class IndexTokenizeField;
} // namespace indexlib::document

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::index {

enum TokenType { tt_text, tt_number, tt_unknown };

class InvertedTestHelper
{
public:
    typedef std::map<uint64_t, docid_t> Key2DocIdMap;
    struct KeyAnswerInDoc {
        KeyAnswerInDoc() : docPayload(0), firstOcc(0), fieldMap(0) {}

        docpayload_t docPayload;
        firstocc_t firstOcc;
        fieldmap_t fieldMap;
        std::vector<pos_t> positionList;
        std::vector<int32_t> fieldIdxList;
        std::vector<pospayload_t> posPayloadList;
    };

    struct KeyAnswer {
        KeyAnswer() : termPayload(0), totalTF(0) {}

        termpayload_t termPayload;
        tf_t totalTF;
        std::vector<docid_t> docIdList;
        std::vector<tf_t> tfList;
        std::map<docid_t, KeyAnswerInDoc> inDocAnswers;
    };

    typedef std::map<std::string, KeyAnswer> PostingAnswerMap;
    typedef std::map<dictkey_t, std::string> HashKeyToStrMap;
    struct SectionAnswer {
        SectionAnswer() : firstPos(0) {}
        SectionAnswer(pos_t fPos, const SectionMeta& meta) : firstPos(fPos), sectionMeta(meta) {}

        pos_t firstPos;
        SectionMeta sectionMeta;
    };

    // string format: "docId;fieldId;sectionId"
    typedef std::map<std::string, SectionAnswer> SectionAnswerMap;

    struct Answer {
        PostingAnswerMap postingAnswerMap;
        SectionAnswerMap sectionAnswerMap;
    };

    typedef std::map<std::pair<docid_t, docpayload_t>, std::vector<std::pair<pos_t, pospayload_t>>> PosAnswerMap;

public:
    static void CreateSections(uint32_t sectionNum, std::vector<document::Section*>& sections, docid_t docId,
                               Answer* answer, HashKeyToStrMap& hashKeyToStrMap, TokenType tokenType = tt_text,
                               pos_t* answerBasePosPtr = NULL, autil::mem_pool::Pool* pool = NULL);

    static document::IndexTokenizeField* CreateField(std::vector<document::Section*>& sectionVec,
                                                     autil::mem_pool::Pool* pool = NULL);

    static std::shared_ptr<document::IndexDocument> MakeIndexDocument(autil::mem_pool::Pool* pool,
                                                                      const document::Field* field, docid_t docId,
                                                                      TokenType tokenType = tt_text);
    static std::shared_ptr<document::IndexDocument>
    MakeIndexDocument(autil::mem_pool::Pool* pool, const document::IndexDocument::FieldVector& fieldVec, docid_t docId,
                      TokenType tokenType = tt_text);

    static std::shared_ptr<document::IndexDocument>
    MakeIndexDocument(autil::mem_pool::Pool* pool, const document::IndexDocument::FieldVector& fieldVec, docid_t docId,
                      Answer* answer, const HashKeyToStrMap& hashKeyToStrMap, TokenType tokenType = tt_text,
                      uint32_t* basePos = NULL);

    static void MakeIndexDocuments(autil::mem_pool::Pool* pool,
                                   std::vector<std::shared_ptr<document::IndexDocument>>& indexDocs, uint32_t docNum,
                                   docid_t begDocId, Answer* answer, TokenType tokenType = tt_text);

    static void
    RewriteSectionAttributeInIndexDocuments(std::vector<std::shared_ptr<document::IndexDocument>>& indexDocs,
                                            const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);

    static void UpdateFieldMapInAnswer(const document::IndexDocument::FieldVector& fieldVec, docid_t docId,
                                       std::shared_ptr<indexlibv2::config::PackageIndexConfig>& indexConfig,
                                       Answer* answer);
    static uint64_t GetHashKey(const std::string& key, TokenType tokenType);

    static void BuildMultiSegmentsFromDataStrings(const std::vector<std::string>& dataStr, const std::string& filePath,
                                                  const std::string& indexName, const std::vector<docid_t>& baseDocIds,
                                                  std::vector<PosAnswerMap>& answerMaps,
                                                  std::vector<uint8_t>& compressModes,
                                                  const IndexFormatOption& indexFormatOption);
    static uint8_t BuildOneSegmentFromDataString(const std::string& dataStr, const std::string& filePath,
                                                 docid_t baseDocId, PosAnswerMap& answerMap,
                                                 const IndexFormatOption& indexFormatOption);

    static void PrepareSegmentFiles(const std::shared_ptr<file_system::Directory>& segmentDir, std::string indexPath,
                                    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig,
                                    uint32_t docCount = 1);

    static void PrepareDictionaryFile(const std::shared_ptr<file_system::Directory>& directory,
                                      const std::string& dictFileName,
                                      const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

    static document::Field* MakeField(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                      const std::string& str, autil::mem_pool::Pool* pool = NULL);

    static document::Field* MakeField(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                      const std::string& str, HashKeyToStrMap& hashKeyToStrMap,
                                      autil::mem_pool::Pool* pool = NULL);

protected:
    static void UpdateAnswer(const std::string& key, docid_t docId, pos_t posInc, pospayload_t posPayload,
                             PostingAnswerMap* answerMap);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
