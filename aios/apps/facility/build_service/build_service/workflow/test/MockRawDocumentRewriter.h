#pragma once

#include "gmock/gmock.h"

#include "build_service/workflow/RawDocumentRewriter.h"

namespace build_service { namespace workflow {
class MockRawDocumentRewriter : public RawDocumentRewriter
{
public:
    MockRawDocumentRewriter() = default;
    ~MockRawDocumentRewriter() = default;

    MOCK_METHOD1(rewrite, bool(build_service::document::RawDocument* rawDoc));
};
}} // namespace build_service::workflow
