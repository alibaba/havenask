# Push document status in real time

Sample document format:

```

CMD=add^_

PK=12345321^_

url=http://www.aliyun.com/index.html^_

title=Alibaba Cloud Computing Co., Ltd. ^_

body=xxxxxx xxx^_

time=3123423421^_

multi_value_field=1234^]324^]342^_

bidwords=mp3^\price=35.8^Ptime=13867236221^]mp4^\price=32.8^Ptime=13867236221^_

^^

CMD=delete^_

PK=12345321^_

^^

CMD=update_field^_

PK=12345321^_

time=3123423421^_

^^

CMD=delete_sub^_

PK=12345321^_

SUBPK=12345321001^C12345321002^_

^^

```

The data file contains four commands: add, delete, update_field, and delete_sub. Each command consists of multiple rows and each row is a key-value pair.

Commands are separated by `^^\n`, key-value pairs are separated by `^_\n`, and multiple values are separated by `^]`.



- cmd: specifies the operation on a document.

   - add: adds a document. If the document already exists, it will be deleted and then added.

   - update_field: updates a document. You can only update the attribute field currently.

   - delete: deletes a document. When you delete a document, you must specify the primary key of the document.

   - delete_sub: deletes the content of a specified child table.



<div class="lake-content" typography="classic"><h2 id="zabBj" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Additional considerations</span></h2><ul class="ne-ul" style="margin: 0; padding-left: 23px"><li id="u35b62be1"><span class="ne-text">Do not use built-in characters of Havenask in the document. The following table describes the built-in characters:</span></li></ul>



Encoding | Display format in Emacs or Vi | Input method in Emacs | Input method in Vi

-- | -- | -- | --

"\x1E\n" | ^ (Line feed) | C- q C- 6 | C- v C- 6

"\x1F\n" | ^_(Line feed) | C- q C- 7 | C- v C- 7

"\x1D" | ^] | C-q C-5 | C-v C-5

"\x1C" | ^\ | C-q C-4 | C-v C-4

"\x03" | ^C | C-q C-c | C-v C-c



<ul class="ne-ul" style="margin: 0; padding-left: 23px"><li id="uef07caea"><span class="ne-text">Do not include the multi-value delimiter '\x1D' ('^]') in the data for real-time push notifications.</span> </li><li id="ud5995a71"><span class="ne-text">Do not include the multi-value delimiter '\x1D' ('^]') in a field of the TEXT type.</span> </li></ul></div>
