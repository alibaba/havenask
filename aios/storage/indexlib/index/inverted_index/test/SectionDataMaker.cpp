#include "indexlib/index/inverted_index/test/SectionDataMaker.h"

#include "autil/StringTokenizer.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/rewriter/SectionAttributeRewriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMemIndexer.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, SectionDataMaker);

void SectionDataMaker::BuildOneSegmentSectionData(const shared_ptr<config::ITabletSchema>& schema,
                                                  const string& indexName, const string& sectionStr,
                                                  const DirectoryPtr& dir, segmentid_t segId)
{
    const auto& config = schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName);
    assert(config);
    const auto& indexConfig = dynamic_pointer_cast<config::PackageIndexConfig>(config);
    assert(indexConfig);

    Pool pool(indexlib::index::SectionAttributeFormatter::DATA_SLICE_LEN * 16);

    indexlib::index::SectionAttributeMemIndexer memIndexer(IndexerParameter {});
    auto extractorFactory = std::make_unique<plain::DocumentInfoExtractorFactory>();
    auto status = memIndexer.Init(indexConfig, extractorFactory.get());
    assert(status.IsOK());

    autil::StringTokenizer st(sectionStr, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    docid_t docId = 0;

    indexlibv2::document::SectionAttributeRewriter rewriter;
    rewriter.Init(schema);

    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        auto doc = make_shared<indexlibv2::document::NormalDocument>();
        auto indexDoc = make_shared<IndexDocument>(doc->GetPool());
        doc->SetIndexDocument(indexDoc);
        doc->ModifyDocOperateType(ADD_DOC);

        vector<Field*> fields;
        CreateFieldsFromDocSectionString(*it, fields, doc->GetPool());

        for (size_t i = 0; i < fields.size(); ++i) {
            indexDoc->SetField(fields[i]->GetFieldId(), fields[i]);
        }

        doc->SetDocId(docId++);
        auto status = rewriter.RewriteOneDoc(doc);
        assert(status.IsOK());
        memIndexer.EndDocument(*indexDoc);
    }

    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
    DirectoryPtr segmentDirectory = dir->MakeDirectory(ss.str());

    DirectoryPtr indexDirectory = segmentDirectory->MakeDirectory(INDEX_DIR_NAME);
    status = memIndexer.Dump(&pool, indexDirectory, nullptr);
    assert(status.IsOK());

    indexlibv2::framework::SegmentInfo segInfo;
    segInfo.docCount = docId;
    status = segInfo.Store(segmentDirectory->GetIDirectory());
    assert(status.IsOK());
}

void SectionDataMaker::BuildMultiSegmentsSectionData(const shared_ptr<config::ITabletSchema>& schema,
                                                     const string& indexName, const vector<string>& sectionStrs,
                                                     const DirectoryPtr& rootDir)
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
} // namespace indexlibv2::index
