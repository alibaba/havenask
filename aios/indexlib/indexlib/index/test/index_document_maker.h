#ifndef __INDEXLIB_INDEX_DOCUMENT_MAKER_H
#define __INDEXLIB_INDEX_DOCUMENT_MAKER_H

#include <tr1/memory>
#include <vector>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"
#include "indexlib/common/field_format/section_attribute/section_meta.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/index_document.h"

IE_NAMESPACE_BEGIN(index);

enum TokenType 
{
    tt_text,
    tt_number,
    tt_unknown
};

class IndexDocumentMaker
{
public:
    typedef std::map<uint64_t, docid_t> Key2DocIdMap;
    struct KeyAnswerInDoc
    {
        KeyAnswerInDoc() 
            : docPayload(0)
            , firstOcc(0)
            , fieldMap(0)
        {}

        docpayload_t docPayload;
        firstocc_t firstOcc;
        fieldmap_t fieldMap;
        std::vector<pos_t> positionList;
        std::vector<int32_t> fieldIdxList;
        std::vector<pospayload_t> posPayloadList;
    };

    struct KeyAnswer
    {
        KeyAnswer()
            : termPayload(0)
            , totalTF(0)
        {}

        termpayload_t termPayload;
        tf_t totalTF;
        std::vector<docid_t> docIdList;
        std::vector<tf_t> tfList;
        std::map<docid_t, KeyAnswerInDoc> inDocAnswers;
    };
    
    typedef std::map<std::string, KeyAnswer> PostingAnswerMap;
    typedef std::map<dictkey_t, std::string> HashKeyToStrMap;
    struct SectionAnswer
    {
        SectionAnswer()
            : firstPos(0)
        {}
        SectionAnswer(pos_t fPos, const common::SectionMeta& meta)
            : firstPos(fPos)
            , sectionMeta(meta)
        {}

        pos_t firstPos;
        common::SectionMeta sectionMeta;
    };

    // string format: "docId;fieldId;sectionId"
    typedef std::map<std::string, SectionAnswer> SectionAnswerMap;

    struct Answer
    {
        PostingAnswerMap postingAnswerMap;
        SectionAnswerMap sectionAnswerMap;
    };

public:
    static void CreateSections(uint32_t sectionNum, std::vector<document::Section*>& sections,
                               docid_t docId, Answer *answer, HashKeyToStrMap &hashKeyToStrMap,
                               TokenType tokenType = tt_text, pos_t* answerBasePosPtr = NULL,
                               autil::mem_pool::Pool* pool = NULL);

    static document::IndexTokenizeField* CreateField(std::vector<document::Section*>& sectionVec,
            autil::mem_pool::Pool* pool = NULL);

    static document::IndexDocumentPtr MakeIndexDocument(
            autil::mem_pool::Pool* pool, const document::Field* field, 
            docid_t docId, TokenType tokenType = tt_text);
    static document::IndexDocumentPtr MakeIndexDocument(
            autil::mem_pool::Pool* pool, 
            const document::IndexDocument::FieldVector& fieldVec,
            docid_t docId, TokenType tokenType = tt_text);

    static document::IndexDocumentPtr MakeIndexDocument(
            autil::mem_pool::Pool* pool, 
            const document::IndexDocument::FieldVector& fieldVec,
            docid_t docId, Answer* answer, const HashKeyToStrMap &hashKeyToStrMap,
            TokenType tokenType = tt_text, uint32_t* basePos = NULL);

    static void MakeIndexDocuments(autil::mem_pool::Pool* pool,
                                   std::vector<document::IndexDocumentPtr>& indexDocs,
                                   uint32_t docNum, docid_t begDocId, Answer* answer, 
                                   TokenType tokenType = tt_text);

    static void RewriteSectionAttributeInIndexDocuments(
	std::vector<document::IndexDocumentPtr>& indexDocs,
	const config::IndexPartitionSchemaPtr& schema);
    

    static void AddFieldsToWriter(document::IndexDocument::FieldVector& fieldVec,
                                  IndexWriter& writer, docid_t begDocId = 0, bool empty = false);
    static void AddDocsToWriter(std::vector<document::IndexDocumentPtr>& indexDocs,
                                  IndexWriter& writer);

    static void CleanDir(const std::string& dir);

    static void ResetDir(const std::string& dir);

    static void UpdateFieldMapInAnswer(
            const document::IndexDocument::FieldVector& fieldVec, 
            docid_t docId, config::PackageIndexConfigPtr& indexConfig, 
            Answer* answer);

    // make config for pack/expack index
    static void CreatePackageIndexConfig(config::PackageIndexConfigPtr &indexConf, 
            config::IndexPartitionSchemaPtr& schema,
            uint32_t fieldNum = 10, IndexType indexType = it_pack,
            const std::string &indexName="PackageIndex");

    static void CreateSingleFieldIndexConfig(config::SingleFieldIndexConfigPtr &indexConfig, 
            const std::string &indexName = "TextIndex", 
            IndexType indexType = it_text,
            FieldType type = ft_text,
            optionflag_t optionFlag = OPTION_FLAG_ALL);

    static uint64_t GetHashKey(const std::string &key, TokenType tokenType);

protected:
    static void UpdateAnswer(const std::string &key, docid_t docId, 
                             pos_t posInc, pospayload_t posPayload,
                             PostingAnswerMap* answerMap);
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<IndexDocumentMaker> IndexDocumentMakerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACKAGE_TEXT_INDEX_DOCUMENT_MAKER_H
