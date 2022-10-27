#include "indexlib/index/normal/attribute/test/section_attribute_test_util.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"
#include <autil/mem_pool/SimpleAllocator.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SectionAttributeTestUtil);

SectionAttributeTestUtil::SectionAttributeTestUtil() 
{
}

SectionAttributeTestUtil::~SectionAttributeTestUtil() 
{
}

void SectionAttributeTestUtil::BuildMultiSegmentsData(
        const file_system::DirectoryPtr& rootDirectory, 
	const IndexPartitionSchemaPtr& schema,
	const string& indexName,
        const vector<uint32_t>& docCounts, Answer& answer)
{
    for (size_t i = 0; i < docCounts.size(); ++i)
    {
        BuildOneSegmentData(rootDirectory, i, schema, indexName,
                            docCounts[i], answer);
    }
}

void SectionAttributeTestUtil::BuildOneSegmentData(
        const file_system::DirectoryPtr& rootDirectory, 
        segmentid_t segId,
        const IndexPartitionSchemaPtr& schema,
        const string& indexName,
        uint32_t docCount, Answer& answer)
{
    PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(
	PackageIndexConfig, schema->GetIndexSchema()->GetIndexConfig(indexName));
    
    Pool pool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);

    SectionAttributeWriter writer(packIndexConfig);
    writer.Init(NULL);

    SectionAttributeRewriter rewriter;
    rewriter.Init(schema);
    
    for(docid_t docId = 0; docId < (docid_t)docCount; ++docId)
    {
	NormalDocumentPtr doc(new NormalDocument);
        autil::mem_pool::Pool* pool = doc->GetPool();
	IndexDocumentPtr indexDoc(new IndexDocument(pool));
	doc->SetIndexDocument(indexDoc);
	doc->ModifyDocOperateType(ADD_DOC);
	
        vector<SectionMeta> sectionsOfCurrentDoc;
        uint32_t sectionPerDoc = docId % MAX_SECTION_COUNT_PER_DOC + 1;
        uint32_t sectionPerField = sectionPerDoc / MAX_FIELD_COUNT;
        if (sectionPerField == 0)
        {
            sectionPerField = 1;
        }
            
        for (uint32_t fieldId = 0; fieldId < MAX_FIELD_COUNT; ++fieldId)
        {
            IndexTokenizeField* field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
            field->SetFieldId((fieldid_t)(fieldId));
                    
            for (uint32_t sectionId= 0; sectionId< sectionPerField; ++sectionId)
            {
                Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, 8, pool);
                section->SetLength((section_len_t)((docId + sectionId+ 2) % 2047 + 1));
                section->SetWeight((section_weight_t)((docId + sectionId) % 65536));
                field->AddSection(section);
                        
                SectionMeta meta;
                meta.fieldId = field->GetFieldId();
                meta.length = section->GetLength();
                meta.weight = section->GetWeight();
                    
                sectionsOfCurrentDoc.push_back(meta);
            }
                
	    indexDoc->SetField(fieldId, field);
        }

	doc->SetDocId(docId);
	rewriter.Rewrite(doc);
	
        docid_t globalId = answer.size();
        answer[globalId] = sectionsOfCurrentDoc;
        writer.EndDocument(*indexDoc);
    }
    
    stringstream segPath;
    segPath << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
    file_system::DirectoryPtr segDirectory = rootDirectory->MakeInMemDirectory(segPath.str());
    file_system::DirectoryPtr indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);

    writer.Dump(indexDirectory, &pool);
    indexDirectory->Sync(true);
    WriteSegmentInfo(segDirectory, docCount);
}

void SectionAttributeTestUtil::WriteSegmentInfo(
        const file_system::DirectoryPtr& directory, uint32_t docCount)
{
    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.Store(directory);
}

IE_NAMESPACE_END(index);

