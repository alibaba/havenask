#ifndef __INDEXLIB_INDEX_PRINTER_WORKER_H
#define __INDEXLIB_INDEX_PRINTER_WORKER_H

#include <tr1/memory>
#include <tr1/functional>
#include <vector>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/OptionParser.h>
#include "indexlib/partition/index_partition.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include <fslib/fslib.h>
#include <sys/mman.h>

using namespace fslib;

IE_NAMESPACE_BEGIN(tools);

struct Command {
    Command(const std::string& name_, const std::string& doc_,
            const std::tr1::function<bool(const std::string&)>& func_)
        : name(name_)
        , doc(doc_)
        , func(func_)
    {
    }
    Command(const std::string& name_, const std::string& doc_, const std::vector<Command>& subCommands_)
        : name(name_)
        , doc(doc_)
        , subCommands(subCommands_)
    {
    }
    std::string name;
    std::string doc;
    std::tr1::function<bool(const std::string&)> func;
    std::vector<Command> subCommands;
};

class IndexPrinterWorker
{
    enum SummaryQueryType {SQT_DOCID, SQT_PK};
public:
    IndexPrinterWorker();
    ~IndexPrinterWorker();
public:
    bool ParseArgs(int argc, char* argv[]);
    bool Run();

private:
    void AddOption();

    bool InitRootDir();
    bool InitIndexPartitionOption(const std::string& optionPath);
    void InitLogger(const std::string& log);
    bool InitIndexPartition(bool minimalMemory);
    bool InitIndexReader();

    bool DoQuery(const std::string& query);
    bool DoAnalysis(const std::string& analysis);
    bool DoStatistics(const std::string& stat);
    bool DoValidate(const std::string& validate);
    bool DoMemEstimate(const std::string& optionPath);
    bool DoIndexQuery(const std::string& query);
    bool DoSearch(const std::string& query);
    bool DoCheck(const std::string& check);
    bool DoIndexCheck(const std::string& query);
    void DoSummaryCheck();
    void DoAttributeCheck();

    void DoBuildSortCheck();

    void RewriteOptionsForMemEstimate();

    bool DoCustomTableQuery(const std::string& query);
    bool DoSummaryQuery(const std::string& query, SummaryQueryType type);
    bool DoAttributeQuery(const std::string& query);
    bool DoPKAttributeQuery(const std::string& query);
    bool DoPKAllQuery(const std::string& query);
    bool DoSectionQuery(const std::string& query);
    bool DoDeletionMapQuery(const std::string &query);

    bool DoPrint(const std::string& print);
    fslib::fs::MMapFile* OpenFile(const std::string& filePath);
    bool DoSwitchVersion(const std::string& version);
    bool DoReOpen(const std::string& line);
    bool DoPrintUsage(const std::string& line);
    bool DoQuit(const std::string& line);
    bool DoInteractiveWithoutTTY();
    bool DoInteractive();
    std::vector<Command> CreateCommands();
    std::vector<Command> CreateQueryCommands();
    std::vector<Command> CreateCacheCommands();
    std::vector<Command> CreateFileSystemCommands();
    static bool ExecuteCommand(const std::vector<Command>& commands, const std::string& line);


    bool ParseExpackQuery(const std::string& query, std::string& indexName,
                          bool &isAndRelation, std::vector<std::string> &fieldNameVec);

// for stat query
    bool GetDocCount(uint32_t &docCount);
    bool GenDocumentBoundary(index::IndexReaderPtr &indexReader,
                             std::string &indexName, std::vector<uint32_t> &docBoundary);
    bool SetWorkerDir(const std::string &workerDir);

    bool CheckSummaryField(const std::string &fieldName);

private:
    int32_t DoIndexTermQuery(index::PostingIteratorPtr& postingIter,
                             const std::string& key,
                             const config::IndexConfigPtr& indexConfig,
                             bool printDocInfo,
                             bool printPos,
                             bool printFieldId,
                             bool sortByPK,
                             bool markDeleted);
    int32_t DoCustomizedIndexTermQuery(index::PostingIteratorPtr& postingIter,
                                       const config::IndexConfigPtr& indexConfig,
                                       bool markDeleted);

    bool DoSummaryQueryByDocId(docid_t docId, const std::string& pk,
                        const std::string& fieldName, std::string& summary);

    bool DoSummaryQueryByPk(const std::string& pk, const std::string& fieldName,
                            std::string& summary);

    void FillSummary(const IE_NAMESPACE(document)::SearchSummaryDocument* doc,
                     const std::string& fieldName,
                     std::ostringstream& docStream);

    bool DoAttributeQuery(docid_t docId, const std::string& pk,
                          const std::string& fieldName, std::string& attribute,
                          autil::mem_pool::Pool* pool);

    bool GetSingleAttributeValue(
            const config::AttributeSchemaPtr& attrSchema, const std::string& attrName,
            docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool);

    bool DoBatchCheckPrimaryKey(const std::string& pkFileName);

    void SetAttributeValue(std::string fieldName,
                           std::string value,
                           index::DocInfo* docInfo,
                           index::DocInfoAllocatorPtr allocator);
    void SeekAndFillDocInfo(docid_t docId,
                            std::vector<std::string>& fieldNames,
                            index::DocInfo* docInfo,
                            index::DocInfoAllocatorPtr allocator,
                            autil::mem_pool::Pool* pool);

private:
    autil::OptionParser mOptParser;
    partition::IndexPartition* mIndexPartition;
    partition::IndexPartitionReaderPtr mReader;
    versionid_t mVersionId;
    versionid_t mBaseVersionId;
    std::string mRootPath;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    index::AttributeReaderPtr mPKAttrReader;
    IndexType mPkIndexType;
    bool mQuit;
    bool mUseOfflinePartition;
    bool mUseSub;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPrinterWorker);

IE_NAMESPACE_END(tools);

#endif //__INDEXLIB_INDEX_PRINTER_WORKER_H
