#ifndef ISEARCH_BS_TOOLS_DOCUMENT_READER_WORKER_H
#define ISEARCH_BS_TOOLS_DOCUMENT_READER_WORKER_H

#include <tr1/memory>
#include <tr1/functional>
#include <autil/OptionParser.h>
#include <swift/client/SwiftReader.h>
#include <indexlib/indexlib.h>
#include "build_service/common_define.h"
#include "build_service/util/SwiftClientCreator.h"
#include "tools/document_reader_tools/DocumentPrinter.h"

namespace build_service {
namespace tools {

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

class DocumentReaderWorker
{
    enum SummaryQueryType {SQT_DOCID, SQT_PK};
public:
    DocumentReaderWorker();
    ~DocumentReaderWorker();
public:
    bool ParseArgs(int argc, char* argv[]);
    bool Run();

private:
    void AddOption();
    void InitLogger();
    bool InitDocPrinter();

    bool ReadFromFile(const std::string& source);
    bool ReadFromSwift(const std::string& swiftRoot,
                       const std::string& topic,
                       uint32_t filtermask,
                       uint32_t startTs, uint32_t stopTs,
                       uint32_t from, uint32_t to, size_t count,
                       const std::string& msgFile);
    SWIFT_NS(client)::SwiftReader* CreateSwiftReader(const std::string& swiftRoot,
                                                     const std::string& topic,
                                                     uint32_t filtermask,
                                                     uint32_t from, uint32_t to);

private:
    autil::OptionParser mOptParser;
    util::SwiftClientCreatorPtr mClientCreator;
    DocumentPrinterPtr mPrinter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentReaderWorker);

}
}

#endif //ISEARCH_BS_TOOLS_DOCUMENT_READER_WORKER_H

