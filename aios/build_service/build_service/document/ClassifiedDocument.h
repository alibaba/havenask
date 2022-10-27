#ifndef ISEARCH_BS_CLASSIFIEDDOCUMENT_H
#define ISEARCH_BS_CLASSIFIEDDOCUMENT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/extend_document/classified_document.h>

namespace build_service {

typedef IE_NAMESPACE(document)::Field::FieldTag IndexFieldTag;

namespace document {

typedef IE_NAMESPACE(document)::ClassifiedDocument ClassifiedDocument;
BS_TYPEDEF_PTR(ClassifiedDocument);

}
}

#endif //ISEARCH_BS_CLASSIFIEDDOCUMENT_H
