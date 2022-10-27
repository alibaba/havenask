#ifndef ISEARCH_WORKFLOW_TEST_CUSTOMIZED_INDEX_H
#define ISEARCH_WORKFLOW_TEST_CUSTOMIZED_INDEX_H

#include <build_service/config/ResourceReader.h>
#include <autil/StringUtil.h>
#include <indexlib/indexlib.h>
#include <indexlib/misc/log.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/common.h>
#include <indexlib/misc/exception.h>
#include <indexlib/document/document_factory.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/document/document.h>
#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include <indexlib/document/raw_document/default_raw_document.h>
#include <indexlib/table/table_writer.h>
#include <indexlib/table/table_factory.h>
#include <indexlib/util/key_value_map.h>
#include <indexlib/table/table_resource.h>
#include <indexlib/table/table_merger.h>
#include <indexlib/table/merge_policy.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace workflow {

class MyRawDocument : public IE_NAMESPACE(document)::RawDocument
{
public:
    MyRawDocument();
    ~MyRawDocument();
public:
    bool Init(const IE_NAMESPACE(document)::DocumentInitParamPtr&
              initParam) override
    { return true; }

    void setField(const autil::ConstString &fieldName,
                  const autil::ConstString &fieldValue) override;

    void setFieldNoCopy(const autil::ConstString &fieldName,
                        const autil::ConstString &fieldValue) override
    { setField(fieldName, fieldValue); }

    const autil::ConstString &getField(
        const autil::ConstString &fieldName) const override;

    bool exist(const autil::ConstString &fieldName) const override
    { return false; }

    void eraseField(const autil::ConstString &fieldName) override
    {}

    uint32_t getFieldCount() const override
    { return 1; }

    void clear() override {}

    IE_NAMESPACE(document)::RawDocument *clone() const override;
    IE_NAMESPACE(document)::RawDocument *createNewDocument() const override
    { return clone(); }
    std::string getIdentifier() const override
    { return 0; }
    uint32_t GetDocBinaryVersion() const override
    { return 0; }
    std::string toString() const override;

    void setData(const float* data);
    float* getData() { return mData; }
    size_t EstimateMemory() const override { return sizeof(*this); }
    

public:
    void DoSerialize(autil::DataBuffer &dataBuffer,
                     uint32_t serializedVersion) const override;
    void DoDeserialize(autil::DataBuffer &dataBuffer,
                       uint32_t serializedVersion) override;
    void SetDocId(docid_t docId) override;
    docid_t GetDocId() const override { return mDocId; }

private:
    static autil::ConstString SOURCE;
private:
    float mData[4];
    docid_t mDocId;
    ConstString mDocIdStr;
};

typedef std::tr1::shared_ptr<MyRawDocument> MyRawDocumentPtr;

class MyRawDocumentParser : public IE_NAMESPACE(document)::RawDocumentParser
{
public:
    bool parse(const std::string &docString,
               IE_NAMESPACE(document)::RawDocument &rawDoc) override;
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }        
};

class MyDocumentParserForDemo : public IE_NAMESPACE(document)::DocumentParser
{
public:
    MyDocumentParserForDemo(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);
    ~MyDocumentParserForDemo();
public:
    bool DoInit() override;
    IE_NAMESPACE(document)::DocumentPtr
        Parse(const IE_NAMESPACE(document)::IndexlibExtendDocumentPtr& extendDoc) override;
    IE_NAMESPACE(document)::DocumentPtr
        Parse(const autil::ConstString& serializedData) override;
};


class MyDocumentFactoryForDemo : public IE_NAMESPACE(document)::DocumentFactory
{
public:
    MyDocumentFactoryForDemo() {}
    ~MyDocumentFactoryForDemo() {}
public:
    IE_NAMESPACE(document)::RawDocument* CreateRawDocument(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override
    { return new MyRawDocument(); }

    IE_NAMESPACE(document)::DocumentParser* CreateDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override
    { return new MyDocumentParserForDemo(schema); }

    IE_NAMESPACE(document)::RawDocumentParser* CreateRawDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
        const IE_NAMESPACE(document)::DocumentInitParamPtr& initParam) override
    { return new MyRawDocumentParser(); }
private:
    IE_NAMESPACE(document)::KeyMapManagerPtr mHashKeyMapManager;
};

///////////////////////
class MyTableWriter : public IE_NAMESPACE(table)::TableWriter
{
public:
    MyTableWriter(const IE_NAMESPACE(util)::KeyValueMap& parameters);
    ~MyTableWriter();
public:
    bool DoInit() override;
    BuildResult Build(docid_t docId, const IE_NAMESPACE(document)::DocumentPtr& doc) override;
    bool IsDirty() const override;
    bool DumpSegment(IE_NAMESPACE(table)::BuildSegmentDescription& segmentDescription) override;

public:
    size_t EstimateInitMemoryUse(
        const IE_NAMESPACE(table)::TableWriterInitParamPtr& initParam) const override;
    size_t GetCurrentMemoryUse() const override;
    size_t EstimateDumpMemoryUse() const override;
    size_t EstimateDumpFileSize() const override;
    size_t GetLastConsumedMessageCount() const override;

private:
    bool RecoverFromSegment();
public:
    static bool RecoverFromOneSegment(const IE_NAMESPACE(table)::SegmentMetaPtr& segMeta,
                                      float** data, size_t maxDocCount);
public:
    const static size_t DEFAULT_VALUE_COUNT = 4;
    const static size_t DEFAULT_MAX_DOC_COUNT = 1024;

private:
    float** mData;
    size_t mMaxDocCount;
    bool mIsDirty;
    docid_t mMinDocId;
    docid_t mMaxDocId;
    size_t mLastConsumedMessageCount;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MyTableWriter);

class MyTableResource : public IE_NAMESPACE(table)::TableResource
{
public:
    MyTableResource(const IE_NAMESPACE(util)::KeyValueMap& parameters);
    ~MyTableResource();
public:
    bool Init(const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& segmentMetas,
              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
              const IE_NAMESPACE(config)::IndexPartitionOptions& options) override;
    bool ReInit(const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& segmentMetas) override;
    size_t EstimateInitMemoryUse(
        const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& segmentMetas) const override
    {
        return 0u;
    }
    size_t GetCurrentMemoryUse() const override;
    std::vector<IE_NAMESPACE(table)::SegmentMetaPtr> GetSegmentMetas() const
    { return mSegmentMetas; }
private:
    std::vector<IE_NAMESPACE(table)::SegmentMetaPtr> mSegmentMetas;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MyTableResource);

class MyMergePolicy : public IE_NAMESPACE(table)::MergePolicy
{
public:
    MyMergePolicy()
        : mMaxDocCount(0)
    {}
    ~MyMergePolicy() override {};
public:
    bool DoInit() override;

    std::vector<IE_NAMESPACE(table)::TableMergePlanPtr> CreateMergePlansForFullMerge(
        const std::string& mergeStrategyStr,
        const IE_NAMESPACE(config)::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& segmentMetas,
        const IE_NAMESPACE(PartitionRange)& targetRange) const override;

    std::vector<IE_NAMESPACE(table)::TableMergePlanPtr> CreateMergePlansForIncrementalMerge(
        const std::string& mergeStrategyStr,
        const IE_NAMESPACE(config)::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& segmentMetas,
        const IE_NAMESPACE(PartitionRange)& targetRange) const override;

    bool ReduceMergeTasks(
        const IE_NAMESPACE(table)::TableMergePlanPtr& mergePlan,
        const std::vector<IE_NAMESPACE(table)::MergeTaskDescription>& taskDescriptions,
        const std::vector<IE_NAMESPACE(file_system)::DirectoryPtr>& inputDirectorys,
        const IE_NAMESPACE(file_system)::DirectoryPtr& outputDirectory,
        bool isFailOver) const override;

    std::vector<IE_NAMESPACE(table)::MergeTaskDescription> CreateMergeTaskDescriptions(
        const IE_NAMESPACE(table)::TableMergePlanPtr& mergePlan,
        const IE_NAMESPACE(table)::TableMergePlanResourcePtr& planResource,
        const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& segmentMetas,
        IE_NAMESPACE(table)::MergeSegmentDescription& segmentDescription) const override;
private:
    docid_t mMaxDocCount;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MyMergePolicy);

class MyTableMerger : public IE_NAMESPACE(table)::TableMerger
{
public:
    MyTableMerger();
    ~MyTableMerger() override;
public:
    bool Init(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
        const IE_NAMESPACE(config)::IndexPartitionOptions& options,
        const IE_NAMESPACE(table)::TableMergePlanResourcePtr& mergePlanResources,
        const IE_NAMESPACE(table)::TableMergePlanMetaPtr& mergePlanMeta) override;

    size_t EstimateMemoryUse(
        const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& inPlanSegMetas,
        const IE_NAMESPACE(table)::MergeTaskDescription& taskDescription) const override;

    bool Merge(
        const std::vector<IE_NAMESPACE(table)::SegmentMetaPtr>& inPlanSegMetas,
        const IE_NAMESPACE(table)::MergeTaskDescription& taskDescription,
        const IE_NAMESPACE(file_system)::DirectoryPtr& outputDirectory) override;
private:
    float** mData;
    size_t mMaxDocCount;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MyTableMerger);

class MyTableFactory : public IE_NAMESPACE(table)::TableFactory
{
public:
    MyTableFactory() {}
    ~MyTableFactory() {}
public:
    IE_NAMESPACE(table)::TableWriter* CreateTableWriter(
        const IE_NAMESPACE(util)::KeyValueMap& parameters) override;
    IE_NAMESPACE(table)::TableResource* CreateTableResource(
        const IE_NAMESPACE(util)::KeyValueMap& parameters) override;
    IE_NAMESPACE(table)::TableMerger* CreateTableMerger(
        const IE_NAMESPACE(util)::KeyValueMap& parameters) override;
    IE_NAMESPACE(table)::MergePolicy* CreateMergePolicy(
        const IE_NAMESPACE(util)::KeyValueMap& parameters) override;
    IE_NAMESPACE(table)::TableReader* CreateTableReader(
            const IE_NAMESPACE(util)::KeyValueMap& parameters) override;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MyTableFactory);

}
}

#endif //ISEARCH_WORKFLOW_TEST_CUSTOMIZED_INDEX_H
