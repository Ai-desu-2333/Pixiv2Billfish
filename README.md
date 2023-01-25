# Pixiv2Billfish
 Pixiv Illustrator tags and descriptions imported into Billfish

 Pixiv插画Tag和描述导入Billfish

+ 灵感来自于[@Coder-Sakura](https://github.com/Coder-Sakura) 的 [pixiv2eagle](https://github.com/WriteCode-ChangeWorld/Tools/tree/master/0x09-Pixiv%E6%8F%92%E7%94%BBtag%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5Eagle)
+ 现在可以使用`Pixiv2Billfish`为`Billfish`内的`pixiv`插画添加`标签`和`备注`，从而更好地建造、管理、筛选自己的`pixiv`插画数据库（涩图库）

### 效果预览

![img.png](Images/img2.png)
    
### 使用指南
1. 使用Bilifish创建包含有pixiv插画的素材库
2. 下载源码
3. 进入源码目录，安装依赖
    `pip install -r requirements.txt`
4. 修改`Pixiv2Billfish.py`中`DB_PATH`内容为目标数据库目录

    例如 `DB_PATH = r"C:\pictures\.bf\billfish.db"`

    **注意：** 在路径的 `" "` 外需要添加 `r`

    在windows 环境下，`.bf`目录为系统级隐藏目录，需要在资源管理器中反勾选`“隐藏受保护的操作系统文件”`才可在资源库目录下看到。
    
    提示：为防止意外，请在Billfish分析完全部图片后，退出Billfish，且备份原数据库文件后再运行本程序。
        程序提供了一个测试用的数据库 `billfish.db`。
5. 如果你的网络环境需要使用http代理连接至pixiv，
    需要更改`useProxies = 1`
    并按照格式填写代理链接 `proxies`

    例：
    ```
    useProxies = 1 
    proxies = {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}
    ```
   如果你使用hosts及其他方式访问pixiv，请保持`useProxies = 0`
6. 运行程序`python Pixiv2Billfish.py`

### 高级内容
+ 作为参考，4200张图片，有效图片约2200张，8线程从零写入标签与备注，运行约8分钟
+ 目前本程序会针对Billfish数据库中 文件名为 `PID_xxxx` 后缀名为 `jpg` `png` `gif` `webp` `webm` `zip`文件进行处理。
    
   ` eg. 114514_p1.png , 114514.jpg , 114514_ugoira.gif 114514_p3_ugoira.webp`等格式的文件名，都会被本程序处理。

    具体可在 `get_pid` 函数下进行设置，针对billfish的索引模式，也添加了支持(#1)

+ `标签`会以`Artist:name`形式添加作者名，以方便查找到作者
+ `备注`格式如下:
    ```
    Title:标题
    
    Aritst:作者
    
    UID:作者UID
    
    Comment:原图描述
    ```
  
  + Comment中的内容，会对一些html语法进行过滤，以更加直观，详情见 `get_note` 函数
  + 目前，也会对图片添加原图地址（#2）
+ 针对已经404的图片，标签与备注将会添加`Error:404`以作标注
+ 代码中设置了`WRITE_TAG` `WRITE_NOTE`参数，前者决定是否写入标签，后者决定是否写入备注(添加原图地址功能被包含在写入备注中)，可选值`0(False)`,`1(True)`
+ 为提高运行效率，设置了 `SKIP` 参数，可以跳过已有标签/备注的图片[^1]，可选值`0(False)`,`1(True)`
+ 即使设置 `SKIP = 1` 时，脚本针对海量数据运行效率依旧不佳，故设置了 `START_FILE_NUM` `END_FILE_NUM`两个参数
  + `START_FILE_NUM` 决定从多少个文件之后开始处理 标签/备注 便于增量写入，例如上次处理了5000张图片，本次新收录了3000张，可设置该值为5000以从第5001张图片开始处理，设置为0即从头处理
  + `END_FILE_NUM` 决定写入多少文件后停止，与 `START_FILE_NUM` 搭配使用, 例如`START_FILE_NUM = 5000 , END_FILE_NUM = 1000`时，程序将从第5001张图片开始处理，处理1000张图片后结束
+ `TAG_Thread` `NOTE_Thread`分别为写入标签线程 与 写入备注线程，默认均为8线程，两条进程相互独立 `FOR_TOOL` 为启动写入线程的线程[^2] (套娃)

[^1]: `SKIP = 0`时，程序并不会删除数据库中已经存在的内容，而是直接添加，基于SQLlite的特性，标签中重复的添加将被直接略过，备注的添加将视为更新

[^2]: 不确定这样的实现方法是否合理，但是它跑起来了
### 其他
本人代码水平比较渣，故本程序中可能存在较多代码不规范，实现繁琐之处，也可能存在诸多BUG，欢迎交流改进

## 致谢
+ 感谢[@Coder-Sakura](https://github.com/Coder-Sakura) 的 [pixiv2eagle](https://github.com/WriteCode-ChangeWorld/Tools/tree/master/0x09-Pixiv%E6%8F%92%E7%94%BBtag%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5Eagle) 程序提供的灵感和基础代码框架
