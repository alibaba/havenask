#ifndef __INDEXLIB_FAKE_INDEX_PARTITION_WRITER_BASE_H
#define __INDEXLIB_FAKE_INDEX_PARTITION_WRITER_BASE_H

#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(partition);

class FakeIndexPartitionWriterBase : public IndexPartitionWriter
{
public:
    FakeIndexPartitionWriterBase(const IndexPartitionOptions& options)
        : IndexPartitionWriter(options)
    {}
     ~FakeIndexPartitionWriterBase() {}

public:
     void Open(const std::string& rootDir,
                      const Version& version) {}

     void Close() {}

     void CleanUp(const Version& version) {}

     bool AddDocument(const document::DocumentPtr& doc) {return true;}

     bool UpdateDocument(const document::DocumentPtr& doc) {return true;}

     bool RemoveDocument(docid_t docId) {return true;}
    
     bool RemoveDocument(const document::DocumentPtr& doc)
    {
        return true;
    }

     const PrimaryKeyPtr& GetPrimaryKey() 
    { 
        assert(false); 
        static PrimaryKeyPtr pk;
        return pk;
    }

    document::DocumentPtr RewriteToUpdateDocument(
            const document::DocumentPtr& doc) 
    {
        assert(false);
        return document::DocumentPtr();
    }

     bool RemoveDocumentByPrimaryKey(const std::string& key) 
    {
        return true; 
    }

    bool NeedTrimByDocVersion(const IndexPartitionOptions& option)
    { assert(false); return false; }

    void Dump() {}

    void DumpAndMerge(const document::Locator &locator) {}

    bool IsDirty() const {return false;}

    Version GetVersion() const { return Version(); }

    void Merge(bool optimize = false) {}

    void CleanVersions() { }

    void InitTruncateProfileFields(
            const config::TruncateOptionConfigPtr& truncOptionConfig) {}

};

typedef std::tr1::shared_ptr<FakeIndexPartitionWriterBase> FakeIndexPartitionWriterBasePtr;

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_INDEX_PARTITION_WRITER_BASE_H
