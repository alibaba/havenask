#include "indexlib/index/normal/inverted_index/builtin_index/expack/test/expack_index_document_maker.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, ExpackIndexDocumentMaker);

void ExpackIndexDocumentMaker::MakeIndexDocuments(autil::mem_pool::Pool* pool, vector<IndexDocumentPtr>& indexDocs,
                                                  uint32_t docNum, docid_t beginDocId, Answer* answer,
                                                  PackageIndexConfigPtr& indexConf)
{
    for (uint32_t i = 1; i <= docNum; ++i) {
        HashKeyToStrMap hashKeyToStrMap;
        vector<Section*> sectionVec;
        docid_t docId = beginDocId + i - 1;
        CreateSections(i, sectionVec, docId, answer, hashKeyToStrMap, tt_text, NULL, pool);
        Field* field = CreateField(sectionVec, pool);
        field->SetFieldId(0);

        IndexDocument::FieldVector fieldVec(1, const_cast<Field*>(field));
        IndexDocumentPtr indexDoc = MakeIndexDocument(pool, fieldVec, docId, answer, hashKeyToStrMap);
        UpdateFieldMapInAnswer(fieldVec, docId, indexConf, answer);
        indexDoc->SetDocId(i - 1); // set local docid
        indexDocs.push_back(indexDoc);
    }
}
}} // namespace indexlib::index
