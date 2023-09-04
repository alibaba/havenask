#include "indexlib/document/document_rewriter/DocumentRewriteChain.h"

#include "indexlib/document/document_rewriter/IDocumentRewriter.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace document {
class DocumentRewriteChainTest : public TESTBASE
{
};

class DummyRewriter : public IDocumentRewriter
{
public:
    DummyRewriter() = default;
    size_t GetRewriteTime() const { return _rewriteCount; }
    Status Rewrite(document::IDocumentBatch* batch) override
    {
        _rewriteCount++;
        return Status::OK();
    }

private:
    size_t _rewriteCount = 0;
};

TEST_F(DocumentRewriteChainTest, TestSimpleProcess)
{
    DocumentRewriteChain rewriteChain;
    std::shared_ptr<DummyRewriter> rewriter(new DummyRewriter);
    rewriteChain.AppendRewriter(rewriter);
    ASSERT_TRUE(rewriteChain.Rewrite(nullptr).IsOK());
    ASSERT_EQ(1, rewriter->GetRewriteTime());
}

}} // namespace indexlibv2::document
