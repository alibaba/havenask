#include "indexlib/index/inverted_index/builtin_index/expack/test/ExpackIndexDocumentMaker.h"

#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, ExpackIndexDocumentMaker);

void ExpackIndexDocumentMaker::MakeIndexDocuments(autil::mem_pool::Pool* pool,
                                                  std::vector<std::shared_ptr<document::IndexDocument>>& indexDocs,
                                                  uint32_t docNum, docid_t beginDocId,
                                                  InvertedTestHelper::Answer* answer,
                                                  std::shared_ptr<indexlibv2::config::PackageIndexConfig>& indexConf)
{
    for (uint32_t i = 1; i <= docNum; ++i) {
        InvertedTestHelper::HashKeyToStrMap hashKeyToStrMap;
        std::vector<document::Section*> sectionVec;
        docid_t docId = beginDocId + i - 1;
        InvertedTestHelper::CreateSections(i, sectionVec, docId, answer, hashKeyToStrMap, tt_text, nullptr, pool);
        document::Field* field = InvertedTestHelper::CreateField(sectionVec, pool);
        field->SetFieldId(0);

        document::IndexDocument::FieldVector fieldVec(1, const_cast<document::Field*>(field));
        std::shared_ptr<document::IndexDocument> indexDoc =
            InvertedTestHelper::MakeIndexDocument(pool, fieldVec, docId, answer, hashKeyToStrMap);
        InvertedTestHelper::UpdateFieldMapInAnswer(fieldVec, docId, indexConf, answer);
        indexDoc->SetDocId(i - 1); // set local docid
        indexDocs.push_back(indexDoc);
    }
}

} // namespace indexlib::index
