#include <unistd.h>
#include <errno.h>
#include <sstream>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <iomanip>
#include <tr1/memory>
#include <readline/readline.h>
#include <readline/history.h>
#include <fslib/fslib.h>
#include <autil/TimeUtility.h>
#include <autil/legacy/any.h>
#include <autil/StringTokenizer.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include "tools/document_reader_tools/DocumentReaderWorker.h"

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;

using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

SWIFT_USE_NAMESPACE(client);
SWIFT_USE_NAMESPACE(protocol);

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
BS_NAMESPACE_USE(util);

namespace build_service {
namespace tools {

BS_LOG_SETUP(tools, DocumentReaderWorker);

static const string usage = "Usage: \n"       \
    "\t -s source(fileName or swiftRoot)\n"             \
    "\t -schema schema_file\n"                          \
    "\t -filter etc: f1=1,2,3;f2=4\n"                   \
    "\t -display stc: f1,f2,f3\n"                       \
    "\t\t options below only work for swift source\n"   \
    "\t\t -topic swift_topic\n"                         \
    "\t\t -start start_timestamp\n"                     \
    "\t\t -stop  stop_timestamp\n"                      \
    "\t\t -from  \n"                                    \
    "\t\t -to    \n"                                    \
    "\t\t -to    \n"                                    \
    "\t\t -limit \n"                                    \
    "\t\t -mask  \n"                                    \
    "\t\t --file output file\n"                         \
    "\t\t --pk   \n";


static const string version = "document reader tools 1.0.0";

DocumentReaderWorker::DocumentReaderWorker()
{
    AddOption();
    mClientCreator.reset(new SwiftClientCreator);
}

DocumentReaderWorker::~DocumentReaderWorker()
{

}

void DocumentReaderWorker::AddOption()
{
    mOptParser.addOption("-h", "--help", "help", OptionParser::OPT_HELP, false);
    // TODO: support file source
    mOptParser.addOption("-s", "--source", "source", OptionParser::OPT_STRING, true);
    mOptParser.addOption("-schema", "--schema", "schema", OptionParser::OPT_STRING, true);
    mOptParser.addOption("-l", "--log", "log", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-filter", "--filter", "filter", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-display", "--display", "display", OptionParser::OPT_STRING, false);
    // these options only work for swift source
    mOptParser.addOption("-topic", "--topic", "swift_topic", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-start", "--startTs", "start_timestamp", OptionParser::OPT_UINT32, false);
    mOptParser.addOption("-stop", "--stopTs", "stop_timestamp", OptionParser::OPT_UINT32, false);
    mOptParser.addOption("-from", "--from", "from", OptionParser::OPT_UINT32, false);
    mOptParser.addOption("-to", "--to", "to", OptionParser::OPT_UINT32, false);
    mOptParser.addOption("-limit", "--limit", "limit", OptionParser::OPT_UINT32, false);
    mOptParser.addOption("-mask", "--mask", "mask", OptionParser::OPT_UINT32, false);
    mOptParser.addOption("-f", "--file", "file", OptionParser::OPT_STRING, false);
    mOptParser.addOption("-p", "--pk", "pk", OptionParser::OPT_STRING, false); 

}

bool DocumentReaderWorker::ParseArgs(int argc, char *argv[])
{
    if (argc == 1)
    {
        cout << usage << endl;
        return false;
    }

    bool ret = mOptParser.parseArgs(argc, argv);
    if (!ret)
    {
        cout << usage << endl;
        return false;
    }
    return true;
}

bool DocumentReaderWorker::Run()
{
    InitLogger();

    if (!InitDocPrinter())
    {
        return false;
    }
    
    string source;
    mOptParser.getOptionValue("source", source);
    BS_LOG(INFO, "source: %s", source.c_str());
    if (source.find("zfs://") == string::npos)
    {
        ReadFromFile(source);
        return true;
    }
    
    string topic;
    mOptParser.getOptionValue("swift_topic", topic);
    if (topic.empty())
    {
        cerr << "topic can not be empty" << endl;
        return false;
    }
    
    uint32_t startTs = 0;
    mOptParser.getOptionValue("start_timestamp", startTs);
    if (startTs == 0)
    {
        startTs = TimeUtility::currentTimeInSeconds() - 60 * 30;
    }
    uint32_t stopTs = 0;
    mOptParser.getOptionValue("stop_timestamp", stopTs);
    if (stopTs == 0)
    {
        stopTs = TimeUtility::currentTimeInSeconds();
    }
    uint32_t from = 0;
    mOptParser.getOptionValue("from", from);
    uint32_t to = 65535;
    mOptParser.getOptionValue("to", to);
    uint32_t limit = 2000000000;
    mOptParser.getOptionValue("limit", limit);
    uint32_t filtermask = 0;
    mOptParser.getOptionValue("mask", filtermask);
    string msgFile;
    mOptParser.getOptionValue("file", msgFile);
    
    return ReadFromSwift(source, topic, filtermask, startTs, stopTs, from, to, limit, msgFile);    
}

bool DocumentReaderWorker::InitDocPrinter()
{
    string schemaFile;
    mOptParser.getOptionValue("schema", schemaFile);
    string filter;
    mOptParser.getOptionValue("filter", filter);
    string display;
    mOptParser.getOptionValue("display", display);
    string pkStr;
    mOptParser.getOptionValue("pk", pkStr);    

    DocumentPrinter* printer = new DocumentPrinter;
    if (!printer->Init(filter, display, schemaFile, pkStr))
    {
        return false;
    }
    mPrinter.reset(printer);
    return true;
}

bool DocumentReaderWorker::ReadFromSwift(const string& swiftRoot, const string& topic,
                                         uint32_t filtermask,
                                         uint32_t startTs, uint32_t stopTs,
                                         uint32_t from, uint32_t to, size_t count,
                                         const string& msgFile)
{
    FILE* outfile = NULL;
    if (!msgFile.empty())
    {
        outfile = fopen(msgFile.c_str(), "ab");
        IE_LOG(ERROR, "open file[%s] for backup failed", msgFile.c_str());
    }
    IE_LOG(INFO, "open file[%s] for backup.", msgFile.c_str());
    
    SWIFT_NS(client)::SwiftReader* reader = CreateSwiftReader(swiftRoot, topic, filtermask, from, to);

    int64_t startTimestamp = (int64_t)startTs * 1000000;
    SWIFT_NS(protocol)::ErrorCode ec = reader->seekByTimestamp(startTimestamp, true);
    if (ec != SWIFT_NS(protocol)::ERROR_NONE)
    {
        stringstream ss;
        ss << "seek to " << startTimestamp << " failed, "
           << "error: " << SWIFT_NS(protocol)::ErrorCode_Name(ec);
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }
    BS_LOG(INFO, "SwiftProcessedDocProducer seek to [%ld] at startup", startTimestamp);

    int64_t stopTimestamp = (int64_t)stopTs * 1000000;
    BS_LOG(INFO, "stopTimestamp: %ld", stopTimestamp);
    reader->setTimestampLimit(stopTimestamp, stopTimestamp);
    BS_LOG(INFO, "actual stopTimestamp: %ld", stopTimestamp);

    size_t docCount = 0;
    size_t retryCount = 0;
    size_t msgSize = 0;
    size_t matchedMsgCount = 0;
    bool ret = true;

    while (true)
    {
        docCount++;
        if (docCount > count)
        {
            break;
        }
        
        Message message;
        int64_t timestamp;
        ErrorCode ec = reader->read(timestamp, message);
        if (ec != ERROR_NONE) {
            if (retryCount >= 20)
            {
                string errorMsg = "read from swift fail, errorCode["
                    + SWIFT_NS(protocol)::ErrorCode_Name(ec) + "]";
                BS_LOG(ERROR, "%s, last locator: %ld", errorMsg.c_str(), timestamp);
                ret = false;
                break;
            }
            else
            {
                usleep(50 * 1000);
                retryCount++;
                continue;
            }
        }
        retryCount = 0;
        msgSize = message.data().size();
        DataBuffer dataBuffer(const_cast<char*>(message.data().c_str()), msgSize);
        
        NormalDocumentPtr document;
        dataBuffer.read(document);
        stringstream ss;
        bool ret = mPrinter->PrintDocument(document, ss);
        if (ret)
        {
            matchedMsgCount += 1;
        }
        
        if (ret || (docCount % 10000 == 0))
        {
            cout << "msgId:" << message.msgid()
                 << " msg.ts:" << message.timestamp()
                 << " msg.locator:" << timestamp
                 << " docSize:" << msgSize
                 << " docCount:" << docCount
                 << endl
                 << ss.str(); 
        }
        uint32_t msgLength = msgSize;
        if (ret && outfile)
        {
            if (1u != fwrite((void*)(&msgLength), sizeof(uint32_t), 1, outfile))
            {
                BS_LOG(ERROR, "write msg[%ld]length to file [%s] failed",
                       message.msgid(), msgFile.c_str());
                       ret = false;
                       break;
            }
            
            if (msgSize != fwrite(const_cast<char*>(message.data().c_str()),
                                  sizeof(char), msgSize, outfile))
            {
                BS_LOG(ERROR, "write msg[%ld] to file [%s] failed",
                       message.msgid(), msgFile.c_str());
                ret = false;
                break;
            }
        }
    }
    if (outfile)
    {
        fclose(outfile);
        outfile = NULL;
    }
    BS_LOG(INFO, "matched msg count : %lu", matchedMsgCount);
    return ret;
}

void DocumentReaderWorker::InitLogger()
{
    string log;
    mOptParser.getOptionValue("log", log);
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

SWIFT_NS(client)::SwiftReader* DocumentReaderWorker::CreateSwiftReader(
    const string& swiftRoot, const string& topic, uint32_t filtermask,
    uint32_t from, uint32_t to)
{
    SwiftClient* client = mClientCreator->createSwiftClient(swiftRoot, "");
   
    stringstream ss;
    ss << "topicName=" << topic << ";"
       << "from=" << from << ";"
       << "to=" << to << ";"
       << "uint8FilterMask=" << filtermask << ";"
       << "uint8MaskResult=0";
    SwiftReader* reader = client->createReader(ss.str(), NULL);
    BS_LOG(INFO, "create swift reader config [%s]", ss.str().c_str());
    return reader;
}

bool DocumentReaderWorker::ReadFromFile(const std::string& source)
{
    ifstream in(source);
    if (!in)
    {
        return false;
    }
    while (true)
    {
        string line;
        if (!getline(in, line))
        {
            break;
        }
        if (line.find("====================") == string::npos)
        {
            continue;
        }
        if (!getline(in, line))
        {
            break;
        }
        string dataSizeStr = line.substr(line.find("DataSize:") + 10, line.size() - 1);
        uint32_t dataSize = 0;
        if (!StringUtil::fromString(dataSizeStr, dataSize))
        {
            continue;
        }
        char tmp[6];
        in.read(tmp, 6);
        char buffer[dataSize];
        in.read(buffer, dataSize);
        
        DataBuffer dataBuffer(buffer, dataSize);
        NormalDocumentPtr document;
        dataBuffer.read(document);
        stringstream ss;
        if (!mPrinter->PrintDocument(document, ss))
        {
            continue;
        }
        cout << "=======" << endl
             << ss.str();
    }
    return true;
}

}
}
