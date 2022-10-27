#include <ha3/util/JsonValidator.h>
#include <autil/legacy/jsonizable.h>
#include <suez/turing/common/FileUtil.h>
#include <iostream>

using namespace std;
using namespace autil::legacy;

USE_HA3_NAMESPACE(util);
int main(int argc, char **argv) {
    if (argc == 1) {
        cerr << "Need specific file name!" << endl;
        return -1;
    }
    bool retVal = true;
    for (int i = 1; i < argc; i ++) {
        string fileName(argv[i]);
        retVal = retVal & JsonValidator::validate(fileName);
    }
    if (retVal) {
        return 0;
    } else {
        return 1;
    }
}

BEGIN_HA3_NAMESPACE(util);
 HA3_LOG_SETUP(util, JsonValidator);

JsonValidator::JsonValidator() { 
}

JsonValidator::~JsonValidator() { 
}

bool JsonValidator::validate(const string &filePath) {
    string content = suez::turing::FileUtil::readFile(filePath);
    string errDes;
    if (validateJsonString(content, errDes)) {
        cout << "*** [" << filePath << "] Validate Pass! ***" << endl;
        return true;
    } else {
        cerr << "*** [" << filePath << "] Validate Failed! ***" << endl;
        cerr << errDes << endl;
        return false;
    }
}

bool JsonValidator::validateJsonString(const string &content, string &errDes) {
    try {
        Any a;
        FromJsonString(a, content);
    } catch (const ExceptionBase &e) {
        errDes = e.ToString();
        return false;
    }
    return true;
}
END_HA3_NAMESPACE(util);

