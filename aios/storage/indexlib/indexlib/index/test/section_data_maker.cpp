#include "indexlib/index/test/section_data_maker.h"

#include "autil/StringTokenizer.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_writer.h"
#include "indexlib/index/test/index_test_util.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace index {

IE_LOG_SETUP(index, SectionDataMaker);

void SectionDataMaker::BuildOneSegmentSectionData(const IndexPartitionSchemaPtr& schema, const string& indexName,
                                                  const string& sectionStr, const DirectoryPtr& dir, segmentid_t segId)
{
    PackageIndexConfigPtr indexConfig =
        DYNAMIC_POINTER_CAST(PackageIndexConfig, schema->GetIndexSchema()->GetIndexConfig(indexName));

    Pool pool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);

    SectionAttributeWriter writer(indexConfig);
    writer.Init(/*indexConfig=*/nullptr, /*buildResourceMetrics=*/nullptr);

    autil::StringTokenizer st(sectionStr, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    docid_t docId = 0;
    SectionAttributeRewriter rewriter;
    rewriter.Init(schema);

    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        NormalDocumentPtr doc(new NormalDocument);
        IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
        doc->SetIndexDocument(indexDoc);
        doc->ModifyDocOperateType(ADD_DOC);

        vector<Field*> fields;
        CreateFieldsFromDocSectionString(*it, fields, doc->GetPool());

        for (size_t i = 0; i < fields.size(); ++i) {
            indexDoc->SetField(fields[i]->GetFieldId(), fields[i]);
        }

        doc->SetDocId(docId++);
        rewriter.Rewrite(doc);
        writer.EndDocument(*indexDoc);
    }

    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
    DirectoryPtr segmentDirectory = dir->MakeDirectory(ss.str());

    DirectoryPtr indexDirectory = segmentDirectory->MakeDirectory(INDEX_DIR_NAME);
    writer.Dump(indexDirectory, &pool);

    SegmentInfo segInfo;
    segInfo.docCount = docId;
    segInfo.Store(segmentDirectory);
}

void SectionDataMaker::BuildMultiSegmentsSectionData(const IndexPartitionSchemaPtr& schema, const string& indexName,
                                                     const vector<string>& sectionStrs, const DirectoryPtr& rootDir)
{
    for (size_t i = 0; i < sectionStrs.size(); ++i) {
        BuildOneSegmentSectionData(schema, indexName, sectionStrs[i], rootDir, i);
    }
}

void SectionDataMaker::CreateFieldsFromDocSectionString(const string& docSectionString, vector<Field*>& fields,
                                                        autil::mem_pool::Pool* pool)
{
    autil::StringTokenizer st(docSectionString, ",",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    sectionid_t secId = 0;
    fieldid_t lastFieldId = -1;

    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        fieldid_t fieldId;
        section_len_t length;
        section_weight_t weight;

        istringstream iss(*it);
        iss >> fieldId;
        iss >> length;
        iss >> weight;

        fieldId &= 31;
        length &= (1 << 11) - 1;

        if (fieldId != lastFieldId) {
            lastFieldId = fieldId;

            auto field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
            field->SetFieldId(fieldId);
            fields.push_back(field);
        }

        auto currentField = dynamic_cast<IndexTokenizeField*>(fields.back());
        Section* section = currentField->CreateSection();
        section->SetSectionId(secId);
        section->SetWeight(weight);
        section->SetLength(length);

        secId++;
    }
}

void SectionDataMaker::CreateFakeSectionString(docid_t maxDocId, pos_t maxPos, const vector<string>& strs,
                                               vector<string>& sectStrs)
{
    docid_t maxDocIdInCurSeg;
    uint32_t i = 0;
    for (size_t k = 0; k < strs.size(); k++) {
        if (k < strs.size() - 1) {
            const string& nextSegStr = strs[k + 1]; // the next segment
            istringstream iss(nextSegStr);
            iss >> maxDocIdInCurSeg; // get the first docId in the next segment
            maxDocIdInCurSeg--;
        } else {
            maxDocIdInCurSeg = maxDocId;
        }

        stringstream ss;
        for (; (docid_t)i <= maxDocIdInCurSeg; i++) {
            int secNum = maxPos / 0x7ff + 1;
            for (int j = 0; j < secNum; ++j) {
                ss << j << " 2047 1, "; // format: fieldId sectionLen sectionWeight
            }
            ss << ";";
        }
        sectStrs.push_back(ss.str());
    }
}

string SectionDataMaker::CreateFakeSectionString(docid_t maxDocId, pos_t maxPos)
{
    stringstream ss;
    for (docid_t i = 0; i <= maxDocId; i++) {
        int secNum = maxPos / 0x7ff + 1;
        for (int j = 0; j < secNum; ++j) {
            ss << j << " 2047 1, ";
        }
        ss << ";";
    }
    return ss.str();
}
}} // namespace indexlib::index
