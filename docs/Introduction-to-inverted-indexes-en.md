<div class="lake-content" typography="classic"><h2 id="gD4Vi" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">What is an inverted index?</span></h2><p id="u291aeb99" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><span class="ne-text"><br /></span><span class="ne-text">An inverted index is also called a postings file or an inverted file. The inverted index is an indexing method that is used to store mappings between terms and the positions in a document or a set of documents. The inverted index allows fast full-text searches. The inverted index is the most commonly used data structure in document retrieval systems.  <br /></span><span class="ne-text">You can use inverted indexes to quickly find a list of documents that contain a term and the position of the term in the documents, and obtain other information such as term frequency for analysis.   </span></p><h2 id="IQhVR" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Items stored in an inverted index</span></h2>



Item | Description

-- | --

ttf | Abbreviation of the total term frequency. The total term frequency indicates the total number of times that the term appears in all documents.

df | Abbreviation of the document frequency. The document frequency indicates the total number of documents that contain the term.

tf | Abbreviation of the term frequency. The term frequency indicates the number of times that the term appears in the document.

docid | Abbreviation of the document id. A document ID is a unique identifier of a document in the engine. You can use the document ID to obtain other information about the document that you query.

fieldmap | Abbreviation of the field map. The field map is used to record the fields that contain the term.

section information | You can divide some documents into sections and specify additional information for each section. The information can be retrieved for subsequent processing.

position | You can record the positions of the term in the document.

positionpayload | Abbreviation of the position payload. You can specify payload information for different positions in the document. The payload information can be retrieved for subsequent processing.

docpayload | Abbreviation of the document payload. You can specify additional information for some documents. The information can be retrieved for subsequent processing.

termpayload | Abbreviation of the term payload. You can specify additional information for some terms. The information can be retrieved for subsequent processing.



<p id="uebc11eff" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><br></p><h2 id="ROOg1" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Basic procedure for using an inverted index for retrieval</span></h2><p id="uefab12c0" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><span class="ne-text"><br /></span><span class="ne-text">When you query term M by using the inverted index, the engine queries the dictionary file to find the starting position of term M in the postings file. <br /></span><span class="ne-text">Then, the engine parses the postings list to obtain the following information about term M: TermMeta, DocList, and PositionList. <br /></span><span class="ne-text">TermMeta stores the basic description of the term, including the df, ttf, and termpayload of the term. <br /></span><span class="ne-text">DocList includes information about the documents that contain the term. The information about each document consists of the document ID, the term frequency in the document, the document payload, and the fields that contain the term. <br /></span><span class="ne-text">PositionList includes the position information about the term in each document. The position information includes the specific positions of the term in the documents and positionpayload information.  </span></p><p id="u959f4b30" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><br></p><p id="u40689884" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><img src="https://cdn.nlark.com/lark/0/2018/png/114751/1541056605570-e4f5b3f9-0b89-4fc5-9949-b31c2355621f.png" width="546" id="flyif" class="ne-image"></p></div>