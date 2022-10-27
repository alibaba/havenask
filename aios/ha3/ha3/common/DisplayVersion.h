#ifndef ISEARCH_DISPLAYVERSION_H
#define ISEARCH_DISPLAYVERSION_H

#include <tr1/memory>
#include <ha3/common.h>

BEGIN_HA3_NAMESPACE(common);

class DisplayVersion {
public:
    static bool display(int argc, char** argv);
};

END_HA3_NAMESPACE(common);

#endif // ISEARCH_ACCESSLOG_H
