import re

class MarkdownExtractor(object):
    def __init__(self, remove_hyperlinks: bool = True, remove_images: bool = True) -> None:
        self._remove_hyperlinks = remove_hyperlinks
        self._remove_images = remove_images

    def remove_images(self, content: str) -> str:
        """Get a dictionary of a markdown file from its path."""
        #pattern = r"!{1}\[\[(.*)\]\]"
        pattern = r'!\[.*?\]\(.*?\)'
        content = re.sub(pattern, "", content)
        return content

    def remove_hyperlinks(self, content: str) -> str:
        """Get a dictionary of a markdown file from its path."""
        pattern = r"\[(.*?)\]\((.*?)\)"
        content = re.sub(pattern, r"\1", content)
        return content

    def remove_horizontal_line(self, content: str) -> str:
        pattern1 = r'^-+$[\n\r]?'
        pattern2 = r'^=+$[\n\r]?'

        content = re.sub(pattern1, '', content, flags=re.MULTILINE)
        content = re.sub(pattern2, '', content, flags=re.MULTILINE)

        content = re.sub(r'={3,}', '===', content)  
        content = re.sub(r'-{3,}', '---', content)  
        return content

    def remove_anchor_str(self, content: str) -> str:
        pattern = r'\{#(?:[a-zA-Z0-9]+)\}'

        content = re.sub(pattern, '', content)
        return content

    def remove_double_star(self, content: str) -> str:

        content = re.sub(r'\*\*', '', content)
        return content

    def remove_redundant_space(self, content: str) -> str:
        """
        一个或多个空格或tab改为1个空格
        """
        content = re.sub(r'[ \t]+', ' ', content)
        return content

    def remove_redundant_horizontal_line(self, content: str) -> str:
        """
        3个以上'-'改为'---'
        """
        content = re.sub(r'(-{3,})', '---', content)
        return content

    def remove_escape_char(self, content: str) -> str:
        """
        去掉\，例如'\*'-〉'*'
        """
        content = re.sub(r'\\', '', content)
        return content

    def remove_redundant_return(self, content: str) -> str:
        """
        只有空格和tab的行直接去掉
        3个以上的\n改为2个
        """
        content = re.sub(r'^[ \t]+\n', '', content, flags=re.MULTILINE)
        content = re.sub(r'\n{3,}', '\n\n', content)
        return content

    def extract(self, content: str) -> str:
        if self._remove_images:
            # 去除图片链接
            content = self.remove_images(content)
        if self._remove_hyperlinks:
            # 去除链接url字串，保留[]中内容
            content = self.remove_hyperlinks(content)
        # 去除--- 或 ===
        content = self.remove_horizontal_line(content)
        # 去除文档内链接: {#1234}
        content = self.remove_anchor_str(content)
        # 去除重点符号 **text**
        content = self.remove_double_star(content)
        # 去除多余空格或tab
        content = self.remove_redundant_space(content)
        # 3个以上-改为3个-
        content = self.remove_redundant_horizontal_line(content)
        # 去掉转义字符，\* -》 *
        content = self.remove_escape_char(content)
        # 去掉多余行
        content = self.remove_redundant_return(content)
        
        #过滤掉只有1行的文件
        line_list = content.split("\n")
        line_list = [l for l in line_list if len(l) > 0]
        if len(line_list) == 1 and len(content) < 50: 
            print(f"only has 1 line: {line_list[0]}")
            return ''

        for line in content.split("\n"):
            line = line.strip()
            if len(line) > 0:
                title = line
                break

        return content