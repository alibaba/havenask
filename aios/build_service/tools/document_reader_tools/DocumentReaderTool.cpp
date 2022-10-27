#include <iostream>
#include <fstream>
#include <string>
#include <autil/StringUtil.h>
#include "tools/document_reader_tools/DocumentReaderWorker.h"

using namespace std;
using namespace autil;

BS_NAMESPACE_USE(tools);

int main(int argc, char** argv)
{
    DocumentReaderWorker worker;
    bool ret = worker.ParseArgs(argc, argv);
    if (!ret)
    {
        return 1;
    }
    
    ret = worker.Run();
    if (!ret)
    {
        return 1;
    }

    return 0;
}
