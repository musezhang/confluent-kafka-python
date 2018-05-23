#!/usr/bin/env python
#
# 向私信kafak队列中写入数据
# kafka队列配置信息从配置文件中读取
# 脚本参数：
# -c [configfile] 配置文件
# -i [inputfile] 输入数据,逗号分隔
#


from confluent_kafka import Producer
import sys, getopt, ConfigParser
import json

def loadfileName():
    configfile = ''
    inputfile = ''
    try:
        opts, argvs = getopt.getopt(argv, "c:i:")
    except getopt.GetoptError:
        print 'Usage: message_set.py -i [inputfile] -c [configfile]'
        sys.exit(2)
    for opt, agr in opts:
        if opt == '-i':
            inputfile= arg
        elif opt == '-c'
            configfile = arg
    return (inputfile, configfile)

def parse_args(filename):  
    cf = ConfigParser.ConfigParser()  
    cf.read(filename)  
       
    #线上kafka section  
    online = cf.options("online")  
    
    items = cf.items("online")        
    #read  
    _ip = cf.get("online","ip")  
    _port = cf.getint("online", "port")  
    _topic = cf.getint("online", "topic")  
    return (_ip, _port, _topic) 
  

if __name__ == '__main__':
    inputFile, configFile = loadfileName()

    kafka_ip, kafka_port, kafka_topic = parse_args(configfile)

    f = open(inputfile, 'r')

    msglist = f.readlines()
    f.close()
    #主流程，循环遍历每行文件内容，转换为json写入kafka
    for line in msglist:
        msgItem = line.split(',')
        msgJson = json.dumps(msgItem)
        print msgJson

