import os


#文件夹检查，如不存在则自动创建
def check_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)
