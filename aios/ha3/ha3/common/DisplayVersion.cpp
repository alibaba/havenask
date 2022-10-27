#include <ha3/common/DisplayVersion.h>
#include <iostream>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

bool DisplayVersion::display(int argc, char** argv) {
    if (argc == 2 && (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-v") == 0)) {
        cout << HA3_VERSION << endl;
        return true;
    }
    return false;
}

END_HA3_NAMESPACE(common);
