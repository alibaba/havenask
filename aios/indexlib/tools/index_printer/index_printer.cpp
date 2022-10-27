#include <iostream>
#include <fstream>
#include <string>
#include <autil/StringUtil.h>
#include "tools/index_printer/index_printer_worker.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(tools);


int main(int argc, char** argv)
{
    IndexPrinterWorker worker;
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
