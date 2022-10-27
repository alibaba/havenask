#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <iomanip>
#include <tr1/memory>
#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "tools/index_printer/index_printer_worker.h"
#include "tools/index_printer/metrics_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/util/metrics_center.h"
#include "indexlib/util/proxy_metrics.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/counter/multi_counter.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/accessor/key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "tools/index_printer/compress_method_test.h"
#include "tools/index_printer/doc_info_evaluator.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_comparator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_section_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "tools/index_printer/file_system_executor.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index/normal/inverted_index/customized_index/match_info_posting_iterator.h"
#include "indexlib/testlib/result.h"
#include "indexlib/testlib/raw_document.h"
#include "indexlib/test/simple_table_reader.h"
#include "indexlib/test/partition_metrics.h"
#include "indexlib/merger/segment_directory.h"

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;

using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_BEGIN(tools);
IE_LOG_SETUP(NONE, IndexPrinterWorker);

static const string usage = R"--(
Usage:
    index_printer {-d indexDir} [-o index_partition_options] [options]

Options:
        -h, --help                print this help message.
        -d <path>                 specified the index directory.
        -o, --options <path>      specified the index partition options path.
        -l, --log <path>          specified the config file of log.
        -f, --file <path>         specified output file.
        -w, --workerdir <path>    set working directory for index_printer.
        -i, --interactive         inspect interactively after running script.
                                  in this mode you can also use print/query/cache command.
        -v, --version <param>     load target version <param>.
        -s, --sub                 use sub table.
        -m, --minimal <param>     use minimal memory when load indexPartition<param>.
        --plugin <path>           plugin root path <path>.
        -p, --print <param>       print information about index based on <param>.
                                  param options:
                                      list version : "lv"
                                      index format : "if"
                                      segment infos: "si"
                                      position payload: "pd"
        -a, --analysis <param>    analysis index base on <param>
                                      analysis attribute : attribute:"attributeName"
        -q, --query <query_str>   do search based on <query_str>, query_str format:
                                      search index:             "index index_name:query [--docinfo] [--position [--fieldid]] [--bitmap]"
                                      search summary:           "summary docid[:field_name]"
                                      search summarybypk:       "summarybypk pk[:field_name]"
                                      search attribute:         "attribute docid[:attrubite_name]"
                                      search pk attribute:      "pkattr docid"
                                      search all pk:            "pkall pk"
                                      search section attribute: "section index_name:docid"
                                      search deletionmap:       "deletionmap segment_id"
                                      search custom table:      "query custom $queryStr"

        -c, --check <check_str>   do check based on <check_str>, check_str format:
                                      check index:              "index index_name [--docinfo] [--position [--fieldid]]"
                                      check summary:            "summary"
                                      check attribute:          "attribute"
                                      check build_sort:         "build_sort"
        -cache <cache_str>        do cache based on <cache_str>, only valid in interactive mode, cache_str format:
                                      cache index:              "cache index"
                                      cache summary:            "cache summary"
        -stat <Statistics_str>    do statistics based on <Statistics_str>, only valid in interactive mode, Statistics_str format:
                                      stat index:               "index_name"
        -validate                 do validate whether a doc has been deleted, only support pk
        --memEstimate             do estimate memory use when load version

Examples:
        index_printer -d /path/to/index/ -q "index default:mp3"
        index_printer -d /path/to/index/ -q "search default:mp3"
        index_printer -d /path/to/index/ -q "summary 0"
        index_printer -d /path/to/index/ -q "summary 0:subject"
        index_printer -d /path/to/index/ -q "attribute 0"
        index_printer -d /path/to/index/ -q "attribute 0:company_id"
        index_printer -d /path/to/index/ -q "deletionmap 0"
        index_printer -d /path/to/index/ -q "deletionmap all"
        index_printer -d /path/to/index/ -validate pk3
        index_printer -d /path/to/index/ --plugin /path/to/table_plugin -q "custom k1"
        index_printer -d /path/to/index/ -o /path/to/index_partition_options --memEstimate --baseVersion 3
        index_printer -d /path/to/index/ -a "attribute:serviceId"

Note:
        /path/to/index/ should refer to the path where veresion file exist.
        for example: if index path is /home/admin/index/runtimedata/daogou/generation_0/...
        the path passed to index_printer should be /home/admin/index/runtimedata/daogou/generation_0/partition_*_*
)--";

static vector<Command> gGlobalCommands;

IndexPrinterWorker::IndexPrinterWorker()
    : mIndexPartition(NULL)
    , mVersionId(INVALID_VERSION)
    , mBaseVersionId(INVALID_VERSION)
    , mPkIndexType(it_unknown)
    , mQuit(false)
    , mUseOfflinePartition(false)
    , mUseSub(false)
{
    AddOption();
}

IndexPrinterWorker::~IndexPrinterWorker()
{
    mReader.reset();

    delete mIndexPartition;
    mIndexPartition = NULL;
}

void IndexPrinterWorker::AddOption()
{
    mOptParser.addOption("-d", "", "directory", OptionParser::OPT_STRING, true);
    mOptParser.addOption("-o", "", "options", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-p", "--print", "print", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-q", "--query", "query", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-a", "--analysis", "analysis", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-c", "--check", "check", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-cache", "--cache", "cache", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-stat", "--statistics", "stat", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-validate", "--validate", "validate", OptionParser::OPT_STRING, false);
    mOptParser.addOption("", "--memEstimate", "mem_estimate", OptionParser::STORE_TRUE);
    mOptParser.addOption("-l", "--log", "log", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-f", "--file", "file", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-m", "--minimal", "minimal", OptionParser::STORE_TRUE);
    mOptParser.addOption("-h", "--help", "help", OptionParser::OPT_HELP, false);
    mOptParser.addOption("-i", "--interactive", "interactive", OptionParser::STORE_TRUE);
    mOptParser.addOption("-w", "--workerdir", "workerdir", OptionParser::STORE_TRUE);
    mOptParser.addOption("-v", "--version", "version", (versionid_t)-1);
    mOptParser.addOption("", "--baseVersion", "base_version", (versionid_t)-1);
    mOptParser.addOption("-s", "--sub", "use_sub", false);
    mOptParser.addOption("", "--plugin", "plugin", OptionParser::OPT_STRING, false);
}

bool IndexPrinterWorker::ParseArgs(int argc, char *argv[])
{
    if (argc == 1)
    {
        cout << usage << endl;
        return false;
    }

    bool ret = mOptParser.parseArgs(argc, argv);
    if (!ret)
    {
        cout << "see -h please" << endl;
        return false;
    }
    return true;
}

bool IndexPrinterWorker::SetWorkerDir(const string &workerDir)
{
    if (workerDir.empty())
    {
        return true;
    }
    int ret;
    ret = access(workerDir.c_str(), W_OK);
    if (ret != 0 && errno == ENOENT)
    {
        ret = fslib::fs::FileSystem::mkDir(workerDir, true);
    }
    if (ret != 0)
    {
        return false;
    }
    ret = chdir(workerDir.c_str());
    return (ret == 0);
}

bool IndexPrinterWorker::Run()
{
    string workerDir;
    mOptParser.getOptionValue("workerdir", workerDir);
    if (!SetWorkerDir(workerDir))
    {
        cerr << "set worker dir to " << workerDir << " failed"
             << ", error: " << strerror(errno) << endl;
        return false;
    }

    string log;
    mOptParser.getOptionValue("log", log);
    InitLogger(log);

    mOptParser.getOptionValue("directory", mRootPath);
    if(!InitRootDir())
    {
        cerr<<"InitRootDir failed!" << endl;
        return false;
    }

    int32_t cmdNum = 0;
    string query;
    mOptParser.getOptionValue("query", query);
    cmdNum += query.empty() ? 0 : 1;

    string analysis;
    mOptParser.getOptionValue("analysis", analysis);
    cmdNum += analysis.empty() ? 0 : 1;

    string print;
    mOptParser.getOptionValue("print", print);
    cmdNum += print.empty() ? 0 : 1;

    bool interactive = false;
    mOptParser.getOptionValue("interactive", interactive);
    cmdNum += interactive ? 1 : 0;

    string check;
    mOptParser.getOptionValue("check", check);
    cmdNum += check.empty() ? 0 : 1;

    string cache;
    mOptParser.getOptionValue("cache", cache);
    cmdNum += cache.empty() ? 0 : 1;

    string stat;
    mOptParser.getOptionValue("stat", stat);
    cmdNum += stat.empty() ? 0 : 1;

    string validate;
    mOptParser.getOptionValue("validate", validate);
    cmdNum += validate.empty() ? 0 : 1;

    bool memEstimate = false;
    mOptParser.getOptionValue("mem_estimate", memEstimate);
    cmdNum += memEstimate ? 1 : 0;

    if (cmdNum != 1)
    {
        if (cmdNum == 0)
        {
            cout << "Must specify one of -q -p -i -c -stat -validate --memEstimate. cache only valid in -i mode" << endl;
        }
        else
        {
            cout << "Option -q -p -i -c -stat -validate --memEstimate is mutually-exclusive." << endl;
        }
        return false;
    }

    string optionPath;
    mOptParser.getOptionValue("options", optionPath);
    mOptParser.getOptionValue("minimal", mUseOfflinePartition);
    mOptParser.getOptionValue("version", mVersionId);
    mOptParser.getOptionValue("base_version", mBaseVersionId);
    mOptParser.getOptionValue("use_sub", mUseSub);

    if(!InitIndexPartitionOption(optionPath)) {
        cerr << "InitIndexPartitionOption failed!" << endl;
        return false;
    }

    if (!memEstimate)
    {
        if(!InitIndexPartition(mUseOfflinePartition))
        {
            cerr << "InitIndexPartition failed!" << endl;
            return false;
        }

        if(!InitIndexReader())
        {
            cerr << "InitIndexReader failed!" << endl;
            return false;
        }
    }

    string filePath;
    mOptParser.getOptionValue("file", filePath);
    if (!filePath.empty())
    {
        freopen(filePath.c_str(), "w", stdout);
    }

    try
    {
        if (!query.empty())
        {
            return DoQuery(query);
        } else if(!analysis.empty())
        {
            return DoAnalysis(analysis);
        }
        else if (!check.empty())
        {
            return DoCheck(check);
        }
        else if (!stat.empty())
        {
            return DoStatistics(stat);
        }
        else if (!validate.empty())
        {
            return DoValidate(validate);
        }
        else if (memEstimate)
        {
            return DoMemEstimate(optionPath);
        }
        else if (!print.empty())
        {
            return DoPrint(print);
        }
        else if (interactive)
        {
            return DoInteractiveWithoutTTY();
        }
        else if (!cache.empty())
        {
            cout << "--------------------------------------------------" << endl;
            cout << "cache only valid in interactive(-i) mode!" << endl;
        }
    }
    catch (const ExceptionBase& e)
    {
        fprintf(stderr, "receive exception[%s]", e.what());
        cerr<<"receive exception[" << e.what() << "]" << endl;
    }
    return false;

}

bool IndexPrinterWorker::InitRootDir()
{
    if (!FileSystemWrapper::IsExist(mRootPath))
    {
        cerr << "Index dir[" << mRootPath << "] is not exist." << endl;
        return false;
    }
    if (*(mRootPath.rbegin()) != '/')
    {
        mRootPath += '/';
    }
    return true;
}

bool IndexPrinterWorker::InitIndexPartitionOption(const string& optionPath)
{
    if (!optionPath.empty())
    {
        string optionStr;
        if (!FileSystemWrapper::IsExist(optionPath))
        {
            cerr << "Index partition options file[" << optionPath << "] is not exist." << endl;
            return false;
        }
        try
        {
            FileSystemWrapper::AtomicLoad(optionPath, optionStr);
            FromJsonString(mOptions, optionStr);
        }
        catch (const ExceptionBase& e)
        {
            cerr << "Parse index partition options failed, exception[" << e.what() << "]" << endl;
            return false;
        }
    }

    mOptions.GetOnlineConfig().buildConfig.keepVersionCount = INVALID_KEEP_VERSION_COUNT;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    mOptions.GetOnlineConfig().enableRecoverIndex = false;
    mOptions.TEST_mReadOnly = true;
    return true;
}

void IndexPrinterWorker::InitLogger(const string& log)
{
    if (!log.empty())
    {
        IE_LOG_CONFIG(log.c_str());
    }
    else
    {
        IE_ROOT_LOG_CONFIG();
        IE_ROOT_LOG_SETLEVEL(INFO);
    }
}

bool IndexPrinterWorker::InitIndexPartition(bool useOfflinePartition){
    string pluginRoot;
    mOptParser.getOptionValue("plugin", pluginRoot);
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    partitionResource.taskScheduler.reset(new TaskScheduler());
    partitionResource.indexPluginPath = pluginRoot;
    partitionResource.fileBlockCache.reset(new file_system::FileBlockCache());
    string configStr = util::EnvUtil::GetEnv("RS_BLOCK_CACHE", string(""));
    if (!partitionResource.fileBlockCache->Init(configStr, partitionResource.memoryQuotaController, partitionResource.taskScheduler, nullptr))
    {
        cerr << "init block cache failed with " << configStr << endl;
        return false;
    }

    IndexPartitionSchemaPtr schema;
    if (mVersionId == INVALID_VERSION)
    {
        schema = SchemaAdapter::LoadSchema(mRootPath);
    }
    else
    {
        schema = SchemaAdapter::LoadSchema(mRootPath, mVersionId);
    }

    try
    {
        if (useOfflinePartition)
        {
            if (schema->GetTableType() == tt_customized)
            {
                IE_LOG(ERROR, "open index failed! tableType[tt_customized] "
                       "does not support open in minimal mode");
                return false;
            }
            mOptions.SetIsOnline(false);
            mIndexPartition = new partition::OfflinePartition("", partitionResource);
        }
        else
        {
            if (schema->GetTableType() == tt_customized)
            {
                mIndexPartition = new partition::CustomOnlinePartition("", partitionResource);
            }
            else
            {
                mIndexPartition = new partition::OnlinePartition("", partitionResource);
            }
        }
        IndexPartition::OpenStatus os = mIndexPartition->Open(mRootPath, "", mSchema, mOptions, mVersionId);
        if (os != IndexPartition::OS_OK)
        {
            cerr << "open index partition failed! rootDir[" << mRootPath << "]" << endl;
            return false;
        }

        mSchema = mIndexPartition->GetSchema();
        if (mUseSub)
        {
            mSchema = mSchema->GetSubIndexPartitionSchema();
        }
    }
    catch(const ExceptionBase& e)
    {
        cerr << "open index partition failed! rootDir[" << mRootPath << "], exception[" << e.what() << "]" << endl;
        return false;
    }
    return true;
}

bool IndexPrinterWorker::InitIndexReader()
{
    try
    {
        mReader = mIndexPartition->GetReader();
        if (mSchema->GetTableType() == tt_customized)
        {
            return true;
        }
        if (mUseSub)
        {
            mReader = mReader->GetSubPartitionReader();
        }
        IndexReaderPtr indexReader = mReader->GetPrimaryKeyReader();
        if (indexReader)
        {
            string indexName = indexReader->GetIndexName();
            IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexName);
            if (indexConfig->GetIndexType() == it_primarykey64)
            {
                UInt64PrimaryKeyIndexReaderPtr pkReader =
                    DYNAMIC_POINTER_CAST(UInt64PrimaryKeyIndexReader, indexReader);
                mPKAttrReader = pkReader->GetPKAttributeReader();
                mPkIndexType = it_primarykey64;
            }
            else if (indexConfig->GetIndexType() == it_primarykey128)
            {
                UInt128PrimaryKeyIndexReaderPtr pkReader =
                    DYNAMIC_POINTER_CAST(UInt128PrimaryKeyIndexReader, indexReader);
                mPKAttrReader = pkReader->GetPKAttributeReader();
                mPkIndexType = it_primarykey128;
            }
        }
    }
    catch(const ExceptionBase& e)
    {
        IE_LOG(ERROR, "get index reader failed! exception[%s]", e.what());
        return false;
    }
    return true;
}

bool IndexPrinterWorker::DoQuery(const string& query)
{
    return ExecuteCommand(CreateQueryCommands(), query);
}

bool IndexPrinterWorker::DoAnalysis(const string& analysis)
{
    StringTokenizer st(analysis, ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 2) {
        IE_LOG(ERROR, "invalid analysis param:%s", analysis.c_str());
        return false;
    }
    string analysisType = st[0];
    string analysisName = st[1];
    if (analysisType != "attribute") {
        IE_LOG(ERROR, "invalid analysis type:%s, only support attribute", analysisType.c_str());
        return false;
    }
    auto attrReader = mReader->GetAttributeReader(analysisName);
    if (!attrReader) {
        IE_LOG(ERROR, "invalid analysis : no attribute %s", analysisName.c_str());
        return false;
    }
    auto deletionmapReader = mReader->GetDeletionMapReader();
    auto partitionInfo = mReader->GetPartitionInfo();
    size_t totalDocCount = partitionInfo->GetTotalDocCount();
    map<string, int> attributeAnalysis;
    for (docid_t i = 0; i < (docid_t)totalDocCount; i++) {
        if (deletionmapReader->IsDeleted(i)) {
            continue;
        }
        string attrValue;
        attrReader->Read(i, attrValue);
        attributeAnalysis[attrValue]++;
    }

    cout << "analysis result:" << endl;
    cout << "attrValue:docCount" << endl;
    for (auto iter = attributeAnalysis.begin(); iter != attributeAnalysis.end(); iter++) {
        cout << iter->first << ":" << iter->second << endl;
    }
    return true;
}


bool IndexPrinterWorker::DoStatistics(const string& stat)
{

    StringTokenizer st(stat, ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    string indexName = st[0];
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexName);
    if (!indexConfig)
    {
        cerr << "no such index name: \"" << indexName << "\"" << endl;
        return false;
    }

    IndexReaderPtr indexReader = mReader->GetIndexReader();

    KeyIteratorPtr keyIteratorPtr =indexReader->CreateKeyIterator(indexName);
    if (!keyIteratorPtr) {
        IE_LOG(ERROR, "Create key iterator failed");
        return false;
    }

    bool printFlatPos = false;
    for (size_t i = 1; i < st.getNumTokens(); ++i) {
        if (st[i] == string("flatpos")) {
            printFlatPos = true;
        }
    }
    vector<uint32_t> docBoundary;

    if (printFlatPos) {
        IndexType type = indexConfig->GetIndexType();
        if (!(type == it_pack) && !(type == it_expack))
        {
            cerr << "Index : " << indexName << " has no section attribute.\n"
                 << "make sure it's pack or expack." << endl;
            return false;
        }
	if (!GenDocumentBoundary(indexReader, indexName, docBoundary)) {
	  cerr << "generate document boundary for index : " << indexName
	       << " failed." << endl;
	  return false;
	}
    }
    vector<uint32_t> docMaxPos(docBoundary.size(), 0);

    cout << "--------begin to statistic------------" << endl;
    cout << "term: DocFreq:\t TermFreq:\t tf/df:" << endl;
    string key;
    int64_t totalDocFreq = 0;
    int64_t totalTermFreq = 0;
    int64_t termNum = 0;
    int64_t postingSkipSize = 0;
    int64_t postingListSize = 0;

    vector<uint32_t> firstOccBuffer;
    vector<uint32_t> posBuffer;
    vector<int32_t> dfBuffer;
    CompressMethodTest compressMethodTest(200 * 1024 * 1024);
    DeletionMapReaderPtr delReaderPtr = mReader->GetDeletionMapReader();

    while (keyIteratorPtr->HasNext()) {
        stringstream docstream;
        PostingIteratorPtr postingIteratorPtr(keyIteratorPtr->NextPosting(key));
        TermMeta *termMetaPtr = postingIteratorPtr->GetTermMeta();
        TermPostingInfo postingInfo = postingIteratorPtr->GetTermPostingInfo();
        postingSkipSize += postingInfo.GetPostingSkipSize();
        postingListSize += postingInfo.GetPostingListSize();
        df_t docFreq = termMetaPtr->GetDocFreq();
        tf_t termFreq = termMetaPtr->GetTotalTermFreq();
        totalDocFreq += docFreq;
        totalTermFreq += termFreq;
        ++termNum;
        docstream << "term: ";
        docstream << setiosflags(ios::left) << setw(12) << docFreq;
        docstream << setiosflags(ios::left) << setw(14) << termFreq << "  ";
        docstream << setiosflags(ios::left) << setw(12) << (float)termFreq/docFreq << "  ";
        docid_t currDoc = INVALID_DOCID;

        posBuffer.clear();
        firstOccBuffer.clear();
        dfBuffer.clear();
        if (postingIteratorPtr->HasPosition()) {
            while ((currDoc = postingIteratorPtr->SeekDoc(currDoc)) != INVALID_DOCID) {
                if (delReaderPtr->IsDeleted(currDoc))
                {
                    continue;
                }
                ++currDoc;
                TermMatchData termMatchData;
                postingIteratorPtr->Unpack(termMatchData);
                InDocPositionIteratorPtr inDocPosIter
                    = termMatchData.GetInDocPositionIterator();
                pos_t pos = 0;
                bool isFirstOcc = true;
                pos_t lastPos = 0;
                int32_t df = -1;
                while ((pos = inDocPosIter->SeekPosition(pos)) != INVALID_POSITION)
                {
                    if (isFirstOcc) {
                        firstOccBuffer.push_back(pos);
                        isFirstOcc = false;
                    } else {
                        posBuffer.push_back(pos - lastPos);
                    }
                    lastPos = pos;
                    ++df;
                }
                if (df >= 0)
                {
                    dfBuffer.push_back(df);
                }
                termMatchData.FreeInDocPositionState();
            }
        }
        compressMethodTest.Compress((uint32_t *)&dfBuffer[0], dfBuffer.size());
    }

    cout << "--- termNum:" << termNum << "\t totalDocFreq:" << totalDocFreq \
         << "\t totalTermFreq:" << totalTermFreq << "\t totalTermPerDoc:" \
         << (float)totalTermFreq/totalDocFreq << "---"<< endl;
    cout << "--- postingSkipSize:" << postingSkipSize << "\t postingListSize:" \
         << postingListSize << "---" << endl;
    cout << endl << "--------end to statistic------------" << endl;

    return true;
}

bool IndexPrinterWorker::DoValidate(const string &pkStr)
{
    IndexReaderPtr pkIndexReader = mReader->GetPrimaryKeyReader();
    if (!pkIndexReader)
    {
        cerr << "primary key index does not exist" << endl;
        return false;
    }
#define TYPED_LOOKUP(pkReader) do {                                     \
        if (!pkReader)                                                  \
        {                                                               \
            return false;                                               \
        }                                                               \
        docid_t lastDocId = INVALID_DOCID;                              \
        docid_t docId = pkReader->Lookup(pkStr, lastDocId);             \
        if (docId != INVALID_DOCID)                                     \
        {                                                               \
            cout << "valid" << endl;                                    \
        }                                                               \
        else if (lastDocId != INVALID_DOCID)                            \
        {                                                               \
            cout << "lastDocId:" << lastDocId << " is valid" << endl;   \
        }                                                               \
        else                                                            \
        {                                                               \
            cout << "invalid" << endl;                                  \
        }                                                               \
    } while(0)
    IndexType indexType = pkIndexReader->GetIndexType();
    if (indexType == it_primarykey64)
    {
        UInt64PrimaryKeyIndexReaderPtr pkReader = DYNAMIC_POINTER_CAST(
                UInt64PrimaryKeyIndexReader, pkIndexReader);
        TYPED_LOOKUP(pkReader);
    }
    else if (indexType == it_primarykey128)
    {
        UInt128PrimaryKeyIndexReaderPtr pkReader = DYNAMIC_POINTER_CAST(
                UInt128PrimaryKeyIndexReader, pkIndexReader);
        TYPED_LOOKUP(pkReader);
    }
    else
    {
        assert(false);
        return false;
    }
#undef TYPED_LOOKUP
      return true;
}

bool IndexPrinterWorker::DoMemEstimate(const string& optionPath)
{
    if (optionPath.empty())
    {
        cout << "Must specify option with [-o] when do --memEstimate" << endl;
        return false;
    }
    assert(!mSchema);
    RewriteOptionsForMemEstimate();

    Version version;
    VersionLoader::GetVersion(mRootPath, version, mVersionId);
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
        mRootPath, mOptions, version.GetSchemaVersionId());

    Version lastLoadedVersion;
    if (mBaseVersionId != INVALID_VERSION)
    {
        VersionLoader::GetVersion(mRootPath, lastLoadedVersion, mBaseVersionId);
        IE_LOG(INFO, "mem estimate reopen from base version [%d] to [%d]", mBaseVersionId, mVersionId);
    }

    PartitionMemoryQuotaControllerPtr memQuotaController(
        new PartitionMemoryQuotaController(
            MemoryQuotaControllerCreator::CreateMemoryQuotaController()));
    FileBlockCachePtr fileBlockCache(new file_system::FileBlockCache());
    mOptions.GetBuildConfig().usePathMetaCache = true;
    IndexlibFileSystemPtr fs = FileSystemFactory::Create(
        mRootPath, "", mOptions, NULL, memQuotaController, fileBlockCache);
    // force set SupportMmap = true, becase attribute will allways open with mmap but user will only pass dfs index path to index_printer tool, which not support MMap and will reduce to InMem
    DYNAMIC_POINTER_CAST(IndexlibFileSystemImpl, fs)->GetMountTable()->GetDiskStorage()->mSupportMmap = true;
    DirectoryPtr rootDir = DirectoryCreator::Get(fs, mRootPath, false);
    if (!rootDir)
    {
        return false;
    }

    // init PathMetaContainer in storage to load & cache file meta from deploy meta
    Version::Iterator vIter = version.CreateIterator();
    while (vIter.HasNext())
    {
        segmentid_t segId = vIter.Next();
        if (lastLoadedVersion.HasSegment(segId))
        {
            continue;
        }
        DirectoryPtr segDir = rootDir->GetDirectory(version.GetSegmentDirName(segId), false);
        if (!segDir)
        {
            cout << "Segment [" << version.GetSegmentDirName(segId) << "]"
                 << "not exist in [" << rootDir->GetPath() << "]" << endl;
            return false;
        }
        SegmentFileMeta::Create(segDir, true);
        SegmentFileMeta::Create(segDir, false);
    }

    string pluginRoot;
    mOptParser.getOptionValue("plugin", pluginRoot);
    PluginManagerPtr pluginManager = IndexPluginLoader::Load(
        pluginRoot, mSchema->GetIndexSchema(), mOptions);

    PartitionResourceCalculatorPtr calculator(
        new PartitionResourceCalculator(mOptions.GetOnlineConfig()));
    calculator->Init(rootDir, PartitionWriterPtr(),
                     InMemorySegmentContainerPtr(), pluginManager);

    CounterMap counter;
    size_t diffVersionSize = calculator->EstimateDiffVersionLockSizeWithoutPatch(
        mSchema, rootDir, version, lastLoadedVersion,
        counter.GetMultiCounter("diffVersionSizeWithoutPatch"));
    size_t patchSize = calculator->EstimateLoadPatchMemoryUse(
        mSchema, rootDir, version, lastLoadedVersion);
    counter.GetStateCounter("loadPatchMemoryUse")->Set(patchSize);
    counter.GetStateCounter("__sum__")->Set(diffVersionSize + patchSize);

    cout << counter.ToJsonString(false) << endl;
    return true;
}

void IndexPrinterWorker::RewriteOptionsForMemEstimate()
{
    // rewrite mmap unlock to blockcache
    // because some storage system not support mmap will use inmem instead of mmap unlock
    auto doRewrite = [](const LoadConfig& loadConfig) {
        auto loadStrategy = DYNAMIC_POINTER_CAST(MmapLoadStrategy, loadConfig.GetLoadStrategy());
        if (loadStrategy && !loadStrategy->IsLock())
        {
            const_cast<LoadConfig&>(loadConfig).SetLoadStrategyPtr(
                LoadStrategyPtr(new CacheLoadStrategy()));
        }
    };
    for (auto& loadConfig : mOptions.GetOnlineConfig().loadConfigList.GetLoadConfigs())
    {
        doRewrite(loadConfig);
    }
    doRewrite(mOptions.GetOnlineConfig().loadConfigList.GetDefaultLoadConfig());
}

bool IndexPrinterWorker::GetDocCount(uint32_t &docCount)
{
    Version version;
    try
    {
        VersionLoader::GetVersion(mRootPath, version, mVersionId);
    }
    catch (const ExceptionBase& e)
    {
        cerr << "Get version FAILED. Exception:\n" << e.what() << endl;
        return false;
    }
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mRootPath, version));
    merger::SegmentDirectory::Iterator it = segDir->CreateIterator();
    docCount = 0;
    while (it.HasNext())
    {
        string segPath = it.Next() + SEGMENT_INFO_FILE_NAME;
        SegmentInfo segInfo;
        if (!segInfo.Load(segPath))
        {
            cerr << "Load segment info: " << segPath << " FAILED." << endl;
            return false;
        }
        docCount += segInfo.docCount;
    }
    return true;
}

bool IndexPrinterWorker::GenDocumentBoundary(IndexReaderPtr &indexReader,
        string &indexName, vector<uint32_t> &docBoundary)
{
    assert(indexReader);
    const SectionAttributeReader* sectionReader = indexReader->GetSectionReader(indexName);
    if (!sectionReader)
    {
        cerr << "## No section attribute." << endl;
        return true;
    }
    uint32_t docCount = 0;
    if (!GetDocCount(docCount))
    {
        cerr << "## get segment info (docCount) failed." << endl;
        return false;
    }
    docBoundary.reserve(docCount);
    uint32_t lastPos = 0;
    InDocSectionMetaPtr sectionMeta;
    for (docid_t docId = 0; docId < (docid_t)docCount; ++docId) {
        sectionMeta = sectionReader->GetSection(docId);
        if (!sectionMeta)
        {
            cout << "## Get section meta for doc id: " << docId << " FAILED" << endl;
            docBoundary.push_back(lastPos - 1);
            continue;
        }
        uint32_t sectionCount = sectionMeta->GetSectionCount();
        section_weight_t sectionWeight = 0;
        fieldid_t fieldId = 0;
        section_len_t sectionLength = 0;

        for (uint32_t i = 0; i < sectionCount; ++i)
        {
            sectionMeta->GetSectionMeta(i, sectionWeight, fieldId, sectionLength);
            lastPos += sectionLength - 1;
        }
        docBoundary.push_back(lastPos);
    }
    return true;
}

bool IndexPrinterWorker::DoCheck(const string& check)
{
    StringTokenizer st(check, " ", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    string type = st[0];

    if (type != "build_sort" && !mPKAttrReader)
    {
        cerr << "Error: do check without pk attribute." << endl;
        return false;
    }

    if (type == "index")
    {
        return DoIndexCheck(check.substr(type.length()));
    }
    else if (type == "summary")
    {
        DoSummaryCheck();
    }
    else if (type == "attribute")
    {
        DoAttributeCheck();
    }
    else if (type == "batch_check_pk")
    {
        if (st.getNumTokens() != 2)
        {
            cerr << "invalid paramter for batch_check_pk" << endl;
            return false;
        }
        DoBatchCheckPrimaryKey(st[1]);
    }
    else if (type == "build_sort")
    {
        DoBuildSortCheck();
    }
    // else if (type == "section")
    // {
    //     return DoSectionCheck(check.substr(type.length()));
    // }
    else
    {
        cerr << "Invalid check type: \"" << type << "\"" << endl;
        return false;
    }
    return true;
}

void IndexPrinterWorker::DoBuildSortCheck()
{
    PartitionMeta partMeta = PartitionMeta::LoadPartitionMeta(mRootPath);
    size_t size = partMeta.Size();
    IndexPartitionSchemaPtr partitionSchema = mIndexPartition->GetSchema();
    AttributeSchemaPtr attributeSchema = partitionSchema->GetAttributeSchema();

    DocInfoAllocatorPtr allocator(new DocInfoAllocator());
    MultiComparatorPtr multiComp(new MultiComparator);
    vector<string> fieldNames;
    for (size_t i = 0; i < size; i++)
    {
        const SortDescription &desc = partMeta.GetSortDescription(i);
        const string &fieldName = desc.sortFieldName;
        fieldNames.push_back(fieldName);

        AttributeConfigPtr attributeConfig = attributeSchema->GetAttributeConfig(fieldName);
        FieldType fieldType = attributeConfig->GetFieldType();
        allocator->DeclareReference(fieldName, fieldType);
        Reference* refer = allocator->GetReference(fieldName);
        Comparator *comp =
            ClassTypedFactory<Comparator, ComparatorTyped, Reference*, bool>
            ::GetInstance()->Create(fieldType, refer, desc.sortPattern == sp_desc);
        multiComp->AddComparator(ComparatorPtr(comp));
    }

    PartitionInfoPtr partitionInfo= mReader->GetPartitionInfo();
    index::PartitionMetrics metrics = partitionInfo->GetPartitionMetrics();
    vector<docid_t> baseDocIds = partitionInfo->GetBaseDocIds();
    if (baseDocIds.size() == 0 || metrics.totalDocCount == 0)
    {
        cerr << "no segment exist" << endl;
        return;
    }

    DocInfo* lastDocInfo = allocator->Allocate();
    DocInfo* currentDocInfo = allocator->Allocate();

    size_t baseDocIdsIndex = 0;
    docid_t currBaseDocId = baseDocIds[baseDocIdsIndex];
    size_t errorCount = 0;
    autil::mem_pool::Pool pool;
    for (docid_t i = 0; static_cast<size_t>(i) < metrics.totalDocCount; i++)
    {
        SeekAndFillDocInfo(i, fieldNames, currentDocInfo, allocator, &pool);
        if (i == currBaseDocId)
        {
            while (i == currBaseDocId)
            {
                ++baseDocIdsIndex;
                currBaseDocId = (baseDocIdsIndex < baseDocIds.size()) ?
                                baseDocIds[baseDocIdsIndex] : -1;
            }
        }
        else if (multiComp->LessThan(currentDocInfo, lastDocInfo))
        {
            cerr << "Error: sort order not correct between docId "
                 << i - 1 << " and docId " << i  << endl;
            ++errorCount;
        }
        DocInfo* tmp = lastDocInfo;
        lastDocInfo = currentDocInfo;
        currentDocInfo = tmp;
    }
    cerr << "check finish, total doc count: " << metrics.totalDocCount
         << ", error count: " << errorCount << endl;
}

void IndexPrinterWorker::SeekAndFillDocInfo(
        docid_t docId, vector<string>& fieldNames,
        DocInfo* docInfo, DocInfoAllocatorPtr allocator,
        autil::mem_pool::Pool* pool)
{
    if (pool->getUsedBytes() > 64 * 1024 * 1024)
    {
        pool->reset();
    }
    for (size_t i = 0; i < fieldNames.size(); i++)
    {
        string value;
        mReader->GetAttributeReader(fieldNames[i])->Read(docId, value, pool);
        SetAttributeValue(fieldNames[i], value, docInfo, allocator);
    }
}

void IndexPrinterWorker::SetAttributeValue(string fieldName, string value,
        DocInfo* docInfo, DocInfoAllocatorPtr allocator)
{
    IndexPartitionSchemaPtr partitionSchema = mIndexPartition->GetSchema();
    AttributeSchemaPtr attributeSchema = partitionSchema->GetAttributeSchema();
    AttributeConfigPtr attributeConfig = attributeSchema->GetAttributeConfig(fieldName);
    FieldType fieldType = attributeConfig->GetFieldType();
    Reference* refer = allocator->GetReference(fieldName);
    DocInfoEvaluator *evaluator =
        ClassTypedFactory<DocInfoEvaluator, DocInfoEvaluatorTyped>
        ::GetInstance()->Create(fieldType);

    evaluator->Evaluate(value, docInfo, refer);

}

bool IndexPrinterWorker::DoBatchCheckPrimaryKey(const string &pkFileName)
{
    IndexReaderPtr pkIndexReader = mReader->GetPrimaryKeyReader();
    const string &pkIndexName = pkIndexReader->GetIndexName();
    if (!pkIndexReader)
    {
        cerr << "primary key index does not exist" << endl;
        return false;
    }

    ifstream ifs(pkFileName.c_str());
    if (!ifs.is_open())
    {
        cerr << "failed to open " << pkFileName << endl;
        return false;
    }

    stringstream match;
    stringstream miss;
    uint32_t matchCount = 0;
    uint32_t missCount = 0;
    string pkStr;
    while (getline(ifs, pkStr))
    {
        StringUtil::trim(pkStr);
        Term term(pkStr, pkIndexName);
        PostingIteratorPtr it(pkIndexReader->Lookup(term));
        if (it)
        {
            match << pkStr << "\n\t";
            matchCount++;
        }
        else
        {
            miss << pkStr << "\n\t";
            missCount++;
        }
    }
    cerr << "result:\n" << "\t matchCount = "
         << matchCount << ", missCount = " << missCount << endl;
    cerr << "match result:\n\t" << match.str() << endl;
    cerr << "miss result:\n\t" << miss.str() << endl;
    return true;
}

bool IndexPrinterWorker::DoSearch(const string& query)
{
    StringTokenizer st(query, ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 2 && st.getNumTokens() != 3)
    {
        cerr << "Invalid search query format:" << query << endl;
        return false;
    }
    string indexName = st[0];
    string termStr = st[1];
    string fieldName = "";
    if (st.getNumTokens() > 2)
    {
        fieldName = st[2];
        if (!CheckSummaryField(fieldName))
        {
            cerr << "invalid summary field: " << fieldName << endl;
            return false;
        }
    }
    // check summary fields
    PostingType pt_type = pt_default;
    IndexReaderPtr indexReader = mReader->GetIndexReader();
    SummaryReaderPtr summaryReader = mReader->GetSummaryReader();
    if (!summaryReader)
    {
        cerr << "no summary" << endl;
        return false;
    }
    Term term(termStr, indexName);
    PostingIteratorPtr postingIter(indexReader->Lookup(term, 1000, pt_type));
    if (!postingIter)
    {
        cout << "## No result for: [" << indexName << ":" << termStr << "]" << endl;
        return true;
    }
    uint32_t summaryCount = mSchema->GetSummarySchema()->GetSummaryCount();
    docid_t docId = 0;
    map<docid_t, string> results;
    while ((docId = postingIter->SeekDoc(docId)) != INVALID_DOCID)
    {
        IE_NAMESPACE(document)::SearchSummaryDocument doc(NULL, summaryCount);
        (void)summaryReader->GetDocument(docId, &doc);
        ostringstream oss;
        FillSummary(&doc, fieldName, oss);
        results[docId] = oss.str();
    }
    // print result
    ostringstream docStream;
    docStream << "search result is:\n";
    for (map<docid_t, string>::const_iterator it = results.begin();
         it != results.end(); it++)
    {
        docStream << it->first << ": " << it->second << "\n";
    }
    cerr << docStream.str() << endl;
    return true;
}

bool IndexPrinterWorker::CheckSummaryField(const string &fieldName)
{
    FieldSchemaPtr fieldSchema = mSchema->GetFieldSchema();
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    return summarySchema && summarySchema->IsInSummary(fieldSchema->GetFieldId(fieldName));
}

bool IndexPrinterWorker::DoIndexQuery(const string& query)
{
    StringTokenizer st0(query, " ", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    bool printPos = false;
    bool printFieldId = false;
    bool printDocInfo = false;
    bool markDeleted = false;
    PostingType pt_type = pt_default;
    assert(st0.getNumTokens() > 0);
    for (size_t i = 1; i < st0.getNumTokens(); ++i)
    {
        if (st0[i] == string("--position"))
        {
            printPos = true;
        }
        else if (st0[i] == string("--fieldid"))
        {
            printFieldId = true;
        }
        else if (st0[i] == string("--docinfo"))
        {
            printDocInfo = true;
        }
        else if (st0[i] == string("--bitmap"))
        {
            pt_type = pt_bitmap;
        }
        else if (st0[i] == string("--mark-delete"))
        {
            markDeleted = true;
        }
        else
        {
            cerr << "Invalid index query options: " << st0[i] << endl;
            cerr << "Try help to get more informations." << endl;
            return false;
        }
    }

    StringTokenizer st(st0[0], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() < 2)
    {
        cerr << "Invalid index query: \"" << query << "\"" << endl;
        return false;
    }


    size_t delimPos = st0[0].find(":");
    string indexName = st0[0].substr(0, delimPos);
    string queryStr = st0[0].substr(delimPos + 1);

    if(!mSchema->GetIndexSchema())
    {
        cerr << "no index schema" << endl;
        return false;
    }

    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexName);
    if (!indexConfig)
    {
        cerr << "no such index name: \"" << indexName << "\"" << endl;
        return false;
    }
    IndexReaderPtr indexReader = mReader->GetIndexReader();

    Term term(queryStr, indexName);

    PostingIteratorPtr postingIter(indexReader->Lookup(term, 1000, pt_type));

    if (!postingIter)
    {
        cout << "## No result." << endl;
        return true;
    }

    string strKey;
    int32_t docNum = 0;

    if (indexConfig->GetIndexType() == it_customized)
    {
        docNum = DoCustomizedIndexTermQuery(postingIter, indexConfig, markDeleted);
    }
    else
    {
        docNum = DoIndexTermQuery(postingIter, strKey, indexConfig,
                              printDocInfo, printPos, printFieldId, false, markDeleted);
    }

    cout << "Total doc count : " << docNum << endl;

    return true;
}


bool IndexPrinterWorker::DoIndexCheck(const string& query)
{
    StringTokenizer st(query, " ", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    bool printPos = false;
    bool printFieldId = false;
    bool printDocInfo = false;
    for (size_t i = 1; i < st.getNumTokens(); ++i)
    {
        if (st[i] == string("--position"))
        {
            printPos = true;
        }
        else if (st[i] == string("--fieldid"))
        {
            printFieldId = true;
        }
        else if (st[i] == string("--docinfo"))
        {
            printDocInfo = true;
        }
        else
        {
            cerr << "Invalid index query options: " << st[i] << endl;
            cerr << "Try help to get more informations." << endl;
            return false;
        }
    }

    string indexName = st[0];
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexName);
    if (!indexConfig)
    {
        cerr << "no such index name: \"" << indexName << "\"" << endl;
        return false;
    }

    IndexReaderPtr indexReader = mReader->GetIndexReader();
    KeyIteratorPtr keyIterator = indexReader->CreateKeyIterator(indexName);

    while (keyIterator->HasNext())
    {
        string strKey;
        PostingIteratorPtr postingIter(keyIterator->NextPosting(strKey));
        DoIndexTermQuery(postingIter, strKey, indexConfig,
                         printDocInfo, printPos, printFieldId, true, false);
    }
    fclose(stdout);
    cout << "Finish check index." << endl;
    return true;
}

int32_t IndexPrinterWorker::DoIndexTermQuery(PostingIteratorPtr& postingIter,
        const string& strKey, const IndexConfigPtr& indexConfig, bool printDocInfo,
        bool printPos, bool printFieldId, bool sortByPk, bool markDeleted)
{
    int32_t docNum = 0;
    TermMeta *termMeta = postingIter->GetTermMeta();
    stringstream termStream;
    termStream << "--- Term meta: ";

    if (!sortByPk)
    {
        termStream << "DF = " << termMeta->GetDocFreq()
             << ", Total TF = " << termMeta->GetTotalTermFreq();
    }
    else
    {
        termStream << "key = " << strKey;
    }
    termStream << ", Term payload = " << termMeta->GetPayload()
         << " ---" << endl;

    if (printDocInfo)
    {
        termStream << "doc: docid(docpayload)  ";
    }
    else
    {
        termStream << "doc: docid  ";
    }

    if (indexConfig->GetIndexType() == it_expack)
    {
        termStream << "firstocc  fieldmap  ";
    }

    if (printDocInfo)
    {
        termStream << "tf     ";
    }

    if (postingIter->HasPosition() && printPos)
    {
        termStream << ":(pos, pospayload";
        if (printFieldId)
        {
            termStream << ", matchfieldid";
        }
        termStream << "); ...";
    }
    termStream << endl;

    termStream << "--------------------------------------------------" << endl;
    docid_t currDoc = INVALID_DOCID;
    vector<pair<string, string> > pkOutput;
    if (!sortByPk)
    {
        cout << termStream.str();
    }
    DeletionMapReaderPtr delReaderPtr = mReader->GetDeletionMapReader();
    while ((currDoc = postingIter->SeekDoc(currDoc)) != INVALID_DOCID)
    {
        stringstream docStream;
        if (delReaderPtr->IsDeleted(currDoc))
        {
            if (markDeleted)
            {
                docStream << "**deleted** ";
            }
            else
            {
                continue;
            }
        }
        ++docNum;
        docStream << "doc: ";
        string pkStr;
        if (sortByPk)
        {
            mPKAttrReader->Read(currDoc, pkStr, NULL);
            docStream << pkStr;
        }
        else
        {
            docStream << currDoc;
        }

        stringstream ss;
        TermMatchData termMatchData;
        postingIter->Unpack(termMatchData);

        if (printDocInfo)
        {
            ss << "(" << (termMatchData.HasDocPayload() ? termMatchData.GetDocPayload() : -1)
               << ")  ";
        }

        if (printDocInfo)
        {
            docStream << setiosflags(ios::left) << setw(17) << ss.str() << "  ";
        }
        else
        {
            docStream << setiosflags(ios::left) << setw(5) << ss.str() << "  ";
        }

        if (indexConfig->GetIndexType() == it_expack)
        {
            docStream << setiosflags(ios::left)
                 << setw(8) << (termMatchData.HasFirstOcc() ? termMatchData.GetFirstOcc() : -1) << "  "
                 << setw(8) << (termMatchData.HasFieldMap() ? termMatchData.GetFieldMap() : -1) << "  ";
        }

        if (printDocInfo)
        {
            docStream << setiosflags(ios::left) << setw(5) << termMatchData.GetTermFreq() << "  ";
        }

        if (postingIter->HasPosition() && printPos)
        {
            docStream << ":";
            InDocPositionIteratorPtr inDocPosIter
                = termMatchData.GetInDocPositionIterator();

            pos_t pos = 0;
            pos = inDocPosIter->SeekPosition(pos);

            while (pos != INVALID_POSITION)
            {
                docStream << "(" << pos; // - basePos;
                int32_t posPayload = -1;
                if (inDocPosIter->HasPosPayload())
                {
                    posPayload = inDocPosIter->GetPosPayload();
                }
                docStream << "," << posPayload;

                if (printFieldId)
                {
                    docStream << "," << inDocPosIter->GetFieldId();
                }
                docStream << ")";
                if ((pos = inDocPosIter->SeekPosition(pos)) == INVALID_POSITION)
                {
                    break;
                }
                docStream << ";";
            }
        }
        termMatchData.FreeInDocPositionState();
        docStream << endl;
        if (sortByPk)
        {
            pkOutput.push_back(make_pair(pkStr, docStream.str()));
        }
        else
        {
            if (docNum != 0)
            {
                cout << docStream.str();
            }
            else
            {
                cout << "## No result." << endl;
            }
        }
        ++currDoc;
    }

    if (sortByPk)
    {
        sort(pkOutput.begin(), pkOutput.end());
        if (pkOutput.size() > 0)
        {
            cout << termStream.str();
        }
        for (size_t i = 0; i < pkOutput.size(); ++i)
        {
            cout << pkOutput[i].second;
        }
        if (pkOutput.size() > 0)
        {
            cout << endl;
        }
    }
    return docNum;
}

int32_t IndexPrinterWorker::DoCustomizedIndexTermQuery(PostingIteratorPtr& postingIter,
        const IndexConfigPtr& indexConfig, bool markDeleted)
{
    int32_t docNum = 0;
    auto implPostingIter = DYNAMIC_POINTER_CAST(MatchInfoPostingIterator, postingIter);
    assert(implPostingIter);
    auto termMeta = implPostingIter->GetTermMeta();
    stringstream termStream;
    termStream << "--- Term meta: ";
    termStream << "DF = " << termMeta->GetDocFreq()
             << ", Total TF = " << termMeta->GetTotalTermFreq();
    termStream << ", Term payload = " << termMeta->GetPayload()
         << " ---" << endl;

    termStream << "doc: docid  " << endl;
    termStream << "--------------------------------------------------" << endl;
    cout << termStream.str();
    DeletionMapReaderPtr delReaderPtr = mReader->GetDeletionMapReader();
    docid_t currDoc = INVALID_DOCID;
    while ((currDoc = implPostingIter->SeekDoc(currDoc)) != INVALID_DOCID)
    {
        stringstream docStream;
        if (delReaderPtr->IsDeleted(currDoc))
        {
            if (markDeleted)
           {
                docStream << "**deleted** ";
            }
            else
            {
                continue;
            }
        }
        ++docNum;
        docStream << "doc: ";
        docStream << currDoc;

        docStream << endl;
        cout << docStream.str();
        ++currDoc;
    }
    if (docNum == 0)
    {
        cout << "## No result." << endl;
    }
    return docNum;
}

bool IndexPrinterWorker::DoCustomTableQuery(const string& query)
{
    assert(mReader);
    TableReaderPtr tableReader = mReader->GetTableReader();
    if (!tableReader)
    {
        cout << "## TableReader is NULL" << endl;
        return false;
    }

    test::SimpleTableReaderPtr simpleTableReader =
        DYNAMIC_POINTER_CAST(test::SimpleTableReader, tableReader);

    if (!simpleTableReader)
    {
        cout << "IndexPrinter only supports TableReader derived from SimpleTableReader" << endl;
        return false;
    }
    testlib::ResultPtr result = simpleTableReader->Search(query);
    if (!result)
    {
        cout << "## no result." << endl;
        return true;
    }
    ostringstream oss;
    size_t matchCount = result->GetDocCount();
    oss << "matchCount: " << matchCount << endl;
    for (size_t i = 0; i < matchCount; ++i)
    {
        testlib::RawDocumentPtr rawDoc = result->GetDoc(i);
        oss << "------- id :" << i << "------------" << endl;
        for (auto it = rawDoc->Begin(); it != rawDoc->End(); ++it)
        {
            oss << it->first << ":" << it->second << endl;
        }
    }
    cout << oss.str() << "------------------------------" << endl;
    return true;
}

bool IndexPrinterWorker::DoSummaryQuery(const string& query, SummaryQueryType type)
{
    StringTokenizer st(query, ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 1 && st.getNumTokens() != 2)
    {
        cerr << "Invalid summary query format:" << query << endl;
        return false;
    }

    string fieldName;
    if (st.getNumTokens() == 2)
    {
        fieldName = st[1];
        if (!CheckSummaryField(fieldName))
        {
            cerr << "Invalid field name for summary query: " << fieldName << endl;
            return false;
        }
    }
    bool ret = true;
    string summary;
    if (type == SQT_DOCID)
    {
        docid_t docId = INVALID_DOCID;
        ret = StringUtil::strToInt32(st[0].c_str(), docId);
        if (!ret)
        {
            cerr << "Invalid doc id for summary query: " << st[0] << endl;
            return false;
        }
        ret = DoSummaryQueryByDocId(docId, "", fieldName, summary);
    }
    else
    {
        ret = DoSummaryQueryByPk(st[0], fieldName, summary);
    }
    cout << "------------------------ summary --------------------------" << endl;
    cout << summary;
    return ret;
}

bool IndexPrinterWorker::DoSummaryQueryByPk(const string& pk,
        const string& fieldName, string& summary)
{
    if (mPkIndexType == it_unknown)
    {
        cerr << "primary key index is not configured, query summary by pk is not supported!!!"
             << endl;
        return false;
    }
    SummaryReaderPtr summaryReader = mReader->GetSummaryReader();
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    IE_NAMESPACE(document)::SearchSummaryDocument doc(NULL, summarySchema->GetSummaryCount());
    bool ret = summaryReader->GetDocumentByPkStr(pk, &doc);
    if (!ret)
    {
        cerr << "## Get summary for pk: " << pk << " FAILED" << endl;
        return false;
    }

    ostringstream docStream;
    docStream << "----------------- PK: " << pk << " --------------------" << endl;
    FillSummary(&doc, fieldName, docStream);
    summary = docStream.str();
    return true;
}

void IndexPrinterWorker::DoSummaryCheck()
{
    Version version = mReader->GetVersion();
    merger::SegmentDirectory segDir(mRootPath, version);
    merger::SegmentDirectory::Iterator it = segDir.CreateIterator();

    DeletionMapReaderPtr delReader = mReader->GetDeletionMapReader();
    vector<pair<string, string> > docVec;

    docid_t baseDocId = 0;
    while (it.HasNext())
    {
        string segPath = it.Next() + SEGMENT_INFO_FILE_NAME;
        SegmentInfo segInfo;
        segInfo.Load(segPath);
        for (int32_t i = 0; i < (int32_t)segInfo.docCount; ++i)
        {
            docid_t docId = baseDocId + i;
            if (delReader->IsDeleted(docId))
            {
                continue;
            }
            string pkStr;
            mPKAttrReader->Read(docId, pkStr, NULL);
            string summary;
            DoSummaryQueryByDocId(docId, pkStr, "", summary);
            docVec.push_back(make_pair(pkStr, summary));
        }
        baseDocId += segInfo.docCount;
    }
    sort(docVec.begin(), docVec.end());

    for (size_t i = 0; i < docVec.size(); i++)
    {
        cout << docVec[i].second;
    }
}

bool IndexPrinterWorker::DoSummaryQueryByDocId(docid_t docId, const string& pk,
        const string& fieldName, string& summary)
{
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    IE_NAMESPACE(document)::SearchSummaryDocument doc(NULL, summarySchema->GetSummaryCount());
    bool ret = mReader->GetSummaryReader()->GetDocument(docId, &doc);

    ostringstream docStream;
    if (pk.empty())
    {
        docStream << "-------------------- docid: " << docId << " ------------------" << endl;
    }
    else
    {
        docStream << "----------------- PK: " << pk << " --------------------" << endl;
    }

    if (ret)
    {
        FillSummary(&doc, fieldName, docStream);
    }
    else
    {
        cerr << "## Get summary for doc id: " << docId << " FAILED" << endl;
        return false;
    }

    summary = docStream.str();
    return true;
}

void IndexPrinterWorker::FillSummary(const IE_NAMESPACE(document)::SearchSummaryDocument* doc,
                                     const string& fieldName,
                                     ostringstream &docStream)
{
    FieldSchemaPtr fieldSchema = mSchema->GetFieldSchema();
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    if (fieldName.empty())
    {
        SummarySchema::Iterator it = summarySchema->Begin();
        for ( ; it != summarySchema->End(); ++it)
        {
            FieldConfigPtr fieldConfig = (*it)->GetFieldConfig();
            const string& fieldName = fieldConfig->GetFieldName();
            fieldid_t fieldId = fieldConfig->GetFieldId();
            summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId(fieldId);
            const autil::ConstString* fieldValue = doc->GetFieldValue(summaryFieldId);
            string summary;
            if (fieldValue != NULL && fieldValue->size() > 0)
            {
                summary.assign(fieldValue->data(), fieldValue->size());
                docStream << fieldName << " : \"" << summary << "\"" << endl;
            }
        }
    }
    else
    {
        fieldid_t fieldId = fieldSchema->GetFieldId(fieldName);
        summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId(fieldId);
        const autil::ConstString* fieldValue = doc->GetFieldValue(summaryFieldId);
        string summary;
        if (fieldValue != NULL && fieldValue->size() > 0)
        {
            summary.assign(fieldValue->data(), fieldValue->size());
            docStream << fieldName << " : \"" << summary << "\"" << endl;
        }
    }
}

bool IndexPrinterWorker::DoAttributeQuery(const string& query)
{
    StringTokenizer st(query, ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 1 && st.getNumTokens() != 2)
    {
        cerr << "Invalid attribute query format:" << query << endl;
        return false;
    }

    string fieldName;
    docid_t docId;
    if (st.getNumTokens() == 2)
    {
        fieldName = st[1];
        const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
        if (!attrSchema || !attrSchema->GetAttributeConfig(fieldName))
        {
            cerr << "Invalid field name for attribute query: " << fieldName << endl;
            return false;
        }
    }
    bool ret = StringUtil::strToInt32(st[0].c_str(), docId);
    if (!ret)
    {
        cerr << "Invalid doc id for attribute query: " << st[0] << endl;
        return false;
    }
    cout << "------------------------ attribute --------------------------" << endl;
    autil::mem_pool::Pool pool;
    string attribute;
    ret = DoAttributeQuery(docId, "", fieldName, attribute, &pool);
    cout << attribute;
    return ret;
}

bool IndexPrinterWorker::DoAttributeQuery(docid_t docId, const string& pk,
        const string& fieldName, string& attribute,
        autil::mem_pool::Pool *pool)
{
    stringstream attrStream;
    if (pk.empty())
    {
        attrStream << "-------------- docid: " << docId << " -----------------" << endl;
    }
    else
    {
        attrStream << "-------------- pk: " << pk << " -----------------" << endl;
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        attrStream << "## No Attribute field in schema!" << endl;
        return false;
    }

    if (fieldName.empty())
    {
        for (AttributeSchema::Iterator it = attrSchema->Begin();
             it != attrSchema->End(); ++it)
        {
            if ((*it)->IsDisable())
            {
                continue;
            }
            const string& attrName = (*it)->GetAttrName();
            string value;
            bool ret = GetSingleAttributeValue(attrSchema, attrName, docId, value, pool);
            if (!ret)
            {
                attrStream << "## Get attribute for doc id: " << docId << " FAILED" << endl;
                attribute = attrStream.str();
                return false;
            }
            attrStream << attrName << " : " << value << endl;
        }
    }
    else
    {
        string value;
        bool ret = GetSingleAttributeValue(attrSchema, fieldName, docId, value, pool);
        if (!ret)
        {
            attrStream << "## Get attribute for doc id: " << docId << " FAILED" << endl;
            attribute = attrStream.str();
            return false;
        }
        attrStream << fieldName << " : " << value << endl;
    }
    attribute = attrStream.str();
    return true;
}

bool IndexPrinterWorker::GetSingleAttributeValue(
    const AttributeSchemaPtr& attrSchema, const string& attrName,
    docid_t docId, string& attrValue, autil::mem_pool::Pool* pool)
{
    assert(mReader);
    assert(attrSchema);

    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(attrName);
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (packAttrConfig) // attribute belongs to pack attribute
    {
        string packName = packAttrConfig->GetAttrName();
        PackAttributeReaderPtr packAttrReader = mReader->GetPackAttributeReader(packName);
        if (!packAttrReader)
        {
            if (mUseOfflinePartition)
            {
                string errorMsg = "Does not support pack attribute in minimalMemory mode";
                cout << errorMsg << endl;
                IE_LOG(ERROR, "%s", errorMsg.c_str());
            }
            return false;
        }
        return packAttrReader->Read(docId, attrName, attrValue, pool);
    }

    AttributeReaderPtr attrReader = mReader->GetAttributeReader(attrName);
    if (!attrReader)
    {
        return false;
    }
    return attrReader->Read(docId, attrValue, pool);
}

void IndexPrinterWorker::DoAttributeCheck()
{
    Version version = mReader->GetVersion();
    merger::SegmentDirectory segDir(mRootPath, version);
    merger::SegmentDirectory::Iterator it = segDir.CreateIterator();

    DeletionMapReaderPtr delReader = mReader->GetDeletionMapReader();
    vector<pair<string, string> > docVec;

    autil::mem_pool::Pool pool;
    docid_t baseDocId = 0;
    while (it.HasNext())
    {
        string segPath = it.Next() + SEGMENT_INFO_FILE_NAME;
        SegmentInfo segInfo;
        segInfo.Load(segPath);
        for (int32_t i = 0; i < (int32_t)segInfo.docCount; ++i)
        {
            docid_t docId = baseDocId + i;
            if (delReader->IsDeleted(docId))
            {
                continue;
            }
            string pkStr;
            mPKAttrReader->Read(docId, pkStr, NULL);
            string attribute;
            DoAttributeQuery(docId, pkStr, "", attribute, &pool);
            docVec.push_back(make_pair(pkStr, attribute));
        }
        baseDocId += segInfo.docCount;
    }
    sort(docVec.begin(), docVec.end());

    for (size_t i = 0; i < docVec.size(); i++)
    {
        cout << docVec[i].second;
    }
}

bool IndexPrinterWorker::DoPKAttributeQuery(const std::string& query)
{
    docid_t docId;
    bool ret = StringUtil::strToInt32(query.c_str(), docId);
    if (!ret)
    {
        cerr << "Invalid docId: " << query << endl;
        return false;
    }


    AttributeReaderPtr attrReader;
    if (!mSchema->GetIndexSchema()->HasPrimaryKeyAttribute())
    {
        cout << "## No primary key attribute." << endl;
        return false;
    }
    IndexReaderPtr indexReader = mReader->GetPrimaryKeyReader();
    assert(indexReader);
    string indexName = indexReader->GetIndexName();
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexName);
    if (indexConfig->GetIndexType() == it_primarykey64)
    {
        UInt64PrimaryKeyIndexReaderPtr pkReader =
            DYNAMIC_POINTER_CAST(UInt64PrimaryKeyIndexReader, indexReader);
        attrReader = pkReader->GetPKAttributeReader();
    }
    else if (indexConfig->GetIndexType() == it_primarykey128)
    {
        UInt128PrimaryKeyIndexReaderPtr pkReader =
            DYNAMIC_POINTER_CAST(UInt128PrimaryKeyIndexReader, indexReader);
        attrReader = pkReader->GetPKAttributeReader();
    }
    else
    {
        assert(false);
    }
    if (!attrReader)
    {
        cerr << "## No primary key attribute." << endl;
        return false;
    }

    string value;
    ret = attrReader->Read(docId, value, NULL);
    if (!ret)
    {
        cerr << "## Get pk attribute for doc id: " << docId << " FAILED" << endl;
        return false;
    }
    cout << "pk attribute: " << value << endl;
    return true;
}

bool IndexPrinterWorker::DoPKAllQuery(const std::string& query)
{
    const string& pkStr = query;

    const PrimaryKeyIndexReaderPtr& pkIndexReader = mReader->GetPrimaryKeyReader();
    assert(pkIndexReader);
    vector<pair<docid_t, bool>> docidPairVec;
    if (!pkIndexReader->LookupAll(pkStr, docidPairVec))
    {
        return false;
    }
    cout << "pk[" << query <<  "] has " << docidPairVec.size() << " results" << endl;
    for (const auto& docidPair : docidPairVec)
    {
        cout << "["
             << (docidPair.second ? "D" : " ")
             << "] "
             << docidPair.first
             << endl;
    }
    return true;
}

bool IndexPrinterWorker::DoSectionQuery(const string& query)
{
    StringTokenizer st(query, ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 2)
    {
        cerr << "Invalid section attribute query format:" << query << endl;
        return false;
    }
    string indexName = st[0];
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(indexName);
    if (!indexConfig)
    {
        cerr << "Invalid index name: " << indexName << endl;
        return false;
    }
    IndexType type = indexConfig->GetIndexType();
    if (!(type == it_pack) && !(type == it_expack))
    {
        cerr << "Index : " << indexName << " has no section attribute.\n"
             << "make sure it's pack or expack." << endl;
        return false;
    }
    docid_t docId;
    bool ret = (docid_t)StringUtil::strToInt32(st[1].c_str(), docId);
    if (!ret)
    {
        cerr << "Invalid doc id: " << st[1] << "." << endl;
        return false;
    }

    IndexReaderPtr indexReader = mReader->GetIndexReader();
    assert(indexReader);
    const SectionAttributeReader* sectionReader = indexReader->GetSectionReader(indexName);
    if (!sectionReader)
    {
        cerr << "## No section attribute." << endl;
        return true;
    }
    InDocSectionMetaPtr sectionMeta = sectionReader->GetSection(docId);
    if (!sectionMeta)
    {
        cout << "## Get section meta for doc id: " << docId << " FAILED" << endl;
        return false;
    }
    uint32_t sectionCount = sectionMeta->GetSectionCount();
    section_weight_t sectionWeight;
    fieldid_t fieldId;
    section_len_t sectionLength;

    stringstream ss;
    cout << "sectionIdx    sectionWeight    fieldId    sectionLength" << endl;
    cout << "-------------------------------------------------------" << endl;

    for (uint32_t i = 0; i < sectionCount; ++i)
    {
        sectionMeta->GetSectionMeta(i, sectionWeight, fieldId, sectionLength);
        cout << setiosflags(ios::left)
             << setw(10) << i << "    " << setw(13) << sectionWeight << "    "
             << setw(7) << fieldId << "    " << setw(13) << sectionLength << endl;
    }

    return true;
}

bool IndexPrinterWorker::DoDeletionMapQuery(const string &query)
{
    StringTokenizer st(query, " ", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    Version version = mReader->GetVersion();
    vector<segmentid_t> segIds;

    if (0 == st.getNumTokens() || st[0] == "all")
    {
        for (size_t i = 0; i < version.GetSegmentCount(); i++)
        {
            segIds.push_back(version[i]);
        }
    }
    else
    {
        for (uint32_t i = 0; i < st.getNumTokens(); i++)
        {
            segmentid_t segId = INVALID_SEGMENTID;
            if (!StringUtil::fromString(st[i], segId) || !version.HasSegment(segId))
            {
                cerr << "invalid segmentId: " << st[i] << endl;
                continue;
            }
            segIds.push_back(segId);
        }
    }
    if (segIds.size() == 0)
    {
        cerr << "##no valid segIds" << endl;
        return false;
    }
    const DeletionMapReaderPtr& delMapReader = mReader->GetDeletionMapReader();
    ostringstream oss;
    for (size_t i = 0; i < segIds.size(); i++)
    {
        uint32_t deletedDocCount = delMapReader->GetDeletedDocCount(segIds[i]);
        oss << "segment_" << segIds[i] << " : " << deletedDocCount << endl;
    }
    cerr << "result for deletionmap query: " << endl;
    cerr << oss.str() << endl;
    return true;
}

bool IndexPrinterWorker::DoPrint(const string& print)
{
    if (print == "lv")
    {
        FileList fileList;
        try
        {
            VersionLoader::ListVersion(mRootPath, fileList);
        }
        catch (const ExceptionBase& e)
        {
            cerr << "list version FAILED. Exception:\n" << e.what() << endl;
            return false;
        }
        for (size_t i = 0; i < fileList.size(); ++i)
        {
            string versionStr;
            string versionPath = mRootPath + fileList[i];
            Version version;
            try
            {
                FileSystemWrapper::AtomicLoad(versionPath, versionStr);
                FromJsonString(version, versionStr);
            }
            catch (const ExceptionBase& e)
            {
                cerr << "Jsonize version :" << versionStr << " FAILED. Exception:\n"
                     << e.what() << endl;
                return false;
            }
            cout << "version " << version.GetVersionId() << ":\n";
            for (uint32_t j = 0; j < version.GetSegmentCount(); ++j)
            {
                cout << "    segment " << version[j] << endl;
            }
            cout << endl;
        }
    }
    else if (print == "if")
    {
        //TODO
        //cout << "Index version format: " << IndexPartition::GetIndexFormatVersion(mRootPath) << endl;
    }
    else if (print == "si")
    {
        Version version;
        try
        {
            VersionLoader::GetVersion(mRootPath, version, mVersionId);
        }
        catch (const ExceptionBase& e)
        {
            cerr << "Get version FAILED. Exception:\n" << e.what() << endl;
            return false;
        }
        merger::SegmentDirectory segDir(mRootPath, version);
        merger::SegmentDirectory::Iterator it = segDir.CreateIterator();
        cout << "version: " << version.GetVersionId() << endl;
        cout << "segmentId    docCount      baseDocId     timeStamp         operationId" << endl;
        cout << "----------------------------------------------------------------------" << endl;
        int32_t segIdx = 0;

        docid_t baseDocId = 0;
        while (it.HasNext())
        {
            string infix = mUseSub ? "/" SUB_SEGMENT_DIR_NAME "/" : "";
            string segPath = it.Next() + infix + SEGMENT_INFO_FILE_NAME;
            SegmentInfo segInfo;
            if (!segInfo.Load(segPath))
            {
                cerr << "Load segment info: " << segPath << " FAILED." << endl;
                return false;
            }
            cout << setiosflags(ios::left)
                 << setw(9) << version[segIdx++] << "    "
                 << setw(10) << segInfo.docCount << "    "
                 << setw(10) << baseDocId << "    "
                 << setw(16) << segInfo.timestamp << "  "
                 << setw(32) << autil::StringUtil::strToHexStr(segInfo.locator.ToString()) << endl;
            baseDocId += segInfo.docCount;
        }
    }
    else if (print == "rc")
    {
        //TODO
        //cout << "Reader count: " << mIndexPartition->GetReaderCount() << endl;
    }
    else
    {
        cerr << "Unsupported print commond: " << print << endl;
        return false;
    }
    return true;
}

fslib::fs::MMapFile* IndexPrinterWorker::OpenFile(const string& filePath)
{
    fslib::fs::MMapFile* mMapFile = FileSystemWrapper::MmapFile(filePath, READ, NULL,
            0, PROT_READ, MAP_SHARED, 0, -1);
    if (!mMapFile->isOpened())
    {
        fslib::ErrorCode ec = mMapFile->getLastError();
        IE_LOG(ERROR, "mmap file[%s] FAILED: [%s]",
               filePath.c_str(),
               FileSystemWrapper::GetErrorString(ec).c_str());
        return NULL;
    }
    return mMapFile;

}

bool IndexPrinterWorker::DoReOpen(const string& line)
{
    if (mUseOfflinePartition)
    {
        string errorMsg = "does not support Reopen in minimalMemory mode";
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        cout << errorMsg << endl;
        return false;
    }
    versionid_t oldVersionId = mReader->GetVersion().GetVersionId();
    versionid_t targetVersionId = INVALID_VERSION;
    if (!line.empty())
    {
        StringUtil::fromString(line, targetVersionId);
    }
    IndexPartition::OpenStatus os = mIndexPartition->ReOpen(false,
            targetVersionId);
    // EQ: catch (const ExceptionBase& e)
    if (os == IndexPartition::OS_FILEIO_EXCEPTION ||
        os == IndexPartition::OS_INDEXLIB_EXCEPTION)
    {
        cout << "ReOpen FAILED. reader version: " << oldVersionId << endl;
        return false;
    }

    IndexPartitionReaderPtr reader = mIndexPartition->GetReader();
    versionid_t newVersionId = reader->GetVersion().GetVersionId();
    if (oldVersionId != newVersionId)
    {
        mReader = reader;
        cout << "ReOpen to version:" << newVersionId << endl;
    }
    else
    {
        cout << "ReOpen did nothing. reader version: " << oldVersionId << endl;
    }
    return true;
}

bool IndexPrinterWorker::DoSwitchVersion(const string& versionIdStr)
{
    versionid_t versionId;
    bool ret = StringUtil::strToInt32(versionIdStr.c_str(), versionId);
    if (!ret)
    {
        cout << "Invalid version id:" << versionIdStr << endl;
        return false;
    }
    Version version;
    try
    {
        VersionLoader::GetVersion(mRootPath, version, versionId);
    }
    catch (const ExceptionBase& e)
    {
        cout << "Can't find version " << versionId << endl;
        cerr << "Exception : " << e.what() << endl;
        return false;
    }

    // TODO
    //IndexPartitionReaderPtr reader = mIndexPartition->GetReader(version);
    IndexPartitionReaderPtr reader = mIndexPartition->GetReader();
    if (!reader)
    {
        cout << "Version " << versionId << " is not in memory now." << endl;
        return true;
    }
    cout << "Switch to version " << versionId << " successed." << endl;
    mReader = reader;
    mVersionId = versionId;
    return true;
}


bool IndexPrinterWorker::DoPrintUsage(const string& line)
{
    cout << usage << endl;
    return true;
}

bool IndexPrinterWorker::DoQuit(const string& line)
{
    mQuit = true;
    return true;
}

static string StripWhite(const string& str)
{
    size_t start = 0;
    while (isspace(str[start])) start++;

    if (str[start] == 0)
        return string();

    size_t end = str.length();
    while (end > start && isspace(str[end])) end--;
    return string(str, start, end - start + 1);
}

bool IndexPrinterWorker::DoInteractiveWithoutTTY()
{
    gGlobalCommands = CreateCommands();

    while (!mQuit)
    {
        string line;
        cout << "index_printer >>";
        std::getline(std::cin, line);
        if (line.empty())
        {
            break;
        }
        string cmd = StripWhite(line);
        if (!cmd.empty()) {
            ExecuteCommand(gGlobalCommands, cmd);
        }
    }

    return true;
}

bool IndexPrinterWorker::ExecuteCommand(const vector<Command>& commands, const string& line) {
    for (size_t i = 0; i < commands.size(); i++)
    {
        if (0 == strncmp(commands[i].name.c_str(), line.c_str(),
                         commands[i].name.length()))
        {
            string next = StripWhite(line);
            next = line.substr(commands[i].name.length());
            next = StripWhite(next);

            if (commands[i].func)
            {
                return commands[i].func(next);
            }
            else
            {
                return ExecuteCommand(commands[i].subCommands, next);
            }
        }
    }

    cout << "Unsupported command! try help." << endl;
    return false;
}

vector<Command> IndexPrinterWorker::CreateCommands()
{
    vector<Command> commands;

    commands.push_back(Command("query", "", CreateQueryCommands()));
    commands.push_back(Command("print", "", tr1::bind(&IndexPrinterWorker::DoPrint, this, _1)));
    commands.push_back(Command("reopen", "", tr1::bind(&IndexPrinterWorker::DoReOpen, this, _1)));
    commands.push_back(Command("switch", "", tr1::bind(&IndexPrinterWorker::DoSwitchVersion, this, _1)));
    commands.push_back(Command("help", "", tr1::bind(&IndexPrinterWorker::DoPrintUsage, this, _1)));
    commands.push_back(Command("cache", "", CreateCacheCommands()));
    commands.push_back(Command("quit", "", tr1::bind(&IndexPrinterWorker::DoQuit, this, _1)));

    commands.push_back(Command("filesystem", "", CreateFileSystemCommands()));

    // maybe, use command alias in the future
    commands.push_back(Command("q", "", tr1::bind(&IndexPrinterWorker::DoQuit, this, _1)));

    return commands;
}

vector<Command> IndexPrinterWorker::CreateQueryCommands()
{
    vector<Command> commands;
    if (mSchema->GetTableType() == tt_customized)
    {
        commands.push_back(Command("custom", "",
                                   tr1::bind(&IndexPrinterWorker::DoCustomTableQuery, this, _1)));
        return commands;
    }
    // simply push back longest match first to ensure matching
    commands.push_back(Command("index", "", tr1::bind(&IndexPrinterWorker::DoIndexQuery, this, _1)));
    commands.push_back(Command("search", "", tr1::bind(&IndexPrinterWorker::DoSearch, this, _1)));
    commands.push_back(Command("summarybypk", "",
                               tr1::bind(&IndexPrinterWorker::DoSummaryQuery, this, _1, SQT_PK)));
    commands.push_back(Command("summary", "",
                               tr1::bind(&IndexPrinterWorker::DoSummaryQuery, this, _1, SQT_DOCID)));
    commands.push_back(Command("attribute", "",
                               tr1::bind(static_cast<bool(IndexPrinterWorker::*)(const string&)>(&IndexPrinterWorker::DoAttributeQuery), this, _1)));
    commands.push_back(Command("pkattr", "",
                               tr1::bind(&IndexPrinterWorker::DoPKAttributeQuery, this, _1)));
    commands.push_back(Command("pkall", "", tr1::bind(&IndexPrinterWorker::DoPKAllQuery, this, _1)));
    commands.push_back(Command("section", "",
                               tr1::bind(&IndexPrinterWorker::DoSectionQuery, this, _1)));
    commands.push_back(Command("deletionmap", "",
                               tr1::bind(&IndexPrinterWorker::DoDeletionMapQuery, this, _1)));
    return commands;
}

vector<Command> IndexPrinterWorker::CreateCacheCommands()
{
    vector<Command> commands;

    commands.push_back(Command("index", "", tr1::bind(&FileSystemExecutor::PrintCacheHit, mIndexPartition, LOAD_CONFIG_INDEX_NAME)));
    commands.push_back(Command("summary", "", tr1::bind(&FileSystemExecutor::PrintCacheHit, mIndexPartition, LOAD_CONFIG_SUMMARY_NAME)));
    return commands;
}

vector<Command> IndexPrinterWorker::CreateFileSystemCommands()
{
    vector<Command> commands;

    commands.push_back(Command("metrics", "", tr1::bind(&FileSystemExecutor::PrintFileSystemMetrics, mIndexPartition, _1)));
    return commands;
}

IE_NAMESPACE_END(tools);
