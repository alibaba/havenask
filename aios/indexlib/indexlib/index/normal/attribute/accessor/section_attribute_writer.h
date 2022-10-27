#ifndef __INDEXLIB_SECTION_ATTRIBUTE_WRITER_H
#define __INDEXLIB_SECTION_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"

DECLARE_REFERENCE_CLASS(document, IndexDocument);

IE_NAMESPACE_BEGIN(index);

class SectionAttributeWriter
{
public:
    SectionAttributeWriter(const config::PackageIndexConfigPtr& config);
    ~SectionAttributeWriter() {}

public:
    void Init(util::BuildResourceMetrics* buildResourceMetrics);
    bool EndDocument(const document::IndexDocument& indexDocument);
    void Dump(const file_system::DirectoryPtr &dir,
              autil::mem_pool::PoolBase* dumpPool);
    AttributeSegmentReaderPtr CreateInMemReader() const;
    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const;
private:
    config::PackageIndexConfigPtr mConfig;
    StringAttributeWriterPtr mAttributeWriter;
    indexid_t mIndexId;
    
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SectionAttributeWriter> SectionAttributeWriterPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_WRITER_H


