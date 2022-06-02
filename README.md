# MapReduce
操作系统选做实验
# 测试方法
## wordcount测试
```powershell
gcc -Wall -Werror -pthread wordcount.c mapreduce.c -o wordcount
./wordcount <测试文件> ...
```
