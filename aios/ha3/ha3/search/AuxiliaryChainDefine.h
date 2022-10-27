#ifndef ISEARCH_AUXILIARYCHAINDEFINE_H
#define ISEARCH_AUXILIARYCHAINDEFINE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Term.h>

BEGIN_HA3_NAMESPACE(search);

#define BITMAP_AUX_NAME "BITMAP"

enum QueryInsertType {
    QI_INVALID,
    QI_BEFORE,
    QI_AFTER,
    QI_OVERWRITE
};

enum SelectAuxChainType {
    SAC_INVALID,
    SAC_DF_BIGGER,
    SAC_DF_SMALLER,
    SAC_ALL
};

typedef std::map<common::Term, df_t> TermDFMap;

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AUXILIARYCHAINDEFINE_H
