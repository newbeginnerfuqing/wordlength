# wordlength
云计算实践期末
我选择了MapReduce
这是一个统计多个文档内单词长度并输出的小程序。不是很复杂，但是帮助我理解了mapreduce的思想。
该程序从文件中读入内容，将单词提取出来，获取单词的长度作为key值，value为1表明该单词已记录。
在reduce函数中将相同长度的单词计数并写入输出文件。
